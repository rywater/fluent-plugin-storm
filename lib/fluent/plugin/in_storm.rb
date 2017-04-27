
module Fluent
  class StormInput < Fluent::Input
    Fluent::Plugin.register_input('storm', self)
    config_param :tag, :string, default: 'storm'
    config_param :interval, :integer, default: 60
    config_param :url, :string, default: 'http://localhost:8080'
    config_param :user, :string, default: nil
    config_param :password, :string, default: nil, secret: true
    config_param :window, :string, default: nil
    config_param :sys, :string, default: nil

    def initialize
      super
      require 'net/http'
      require 'uri'
      require 'json'
    end

    def configure(conf)
      super
    end

    def start
      @loop = Coolio::Loop.new
      @tw = TimerWatcher.new(interval, true, log, &method(:execute))
      @tw.attach(@loop)
      @thread = Thread.new(&method(:run))
      execute
    end

    def shutdown
      @tw.detach
      @loop.stop
      @thread.join
    end

    def run
      @loop.run
    rescue => e
      @log.error 'unexpected error', error: e.to_s
      @log.error_backtrace
    end

    private

    def execute
      @time = Engine.now
      summary_uri = to_uri @url, '/api/v1/topology/summary'
      @log.info("Requesting Storm metrics summary from #{summary_uri}")
      response = do_request(summary_uri)
      response_body = parse_json(response.body)
      response_body['topologies'].each do |topology|
        emit_topology(topology)
      end
    end

    def emit_topology(topology)
      topology_id = topology['encodedId']

      path = get_topology_path(topology_id)

      topology_uri = to_uri(@url, path)
      topology_resp = parse_json(do_request(topology_uri).body)
      # We don't need this data for metrics
      topology_resp.delete('visualizationTable')
      topology_resp.delete('configuration')
      @log.info("Emitting data: #{@tag} #{@time} #{topology_resp}")
      Engine.emit(@tag, @time, topology_resp)
    end

    def get_topology_path(topology_id)
      path = "/api/v1/topology/#{topology_id}"

      if !@window.nil? && !@sys.nil?
        path << "?window=#{@window}&sys=#{@sys}"
      elsif !@window.nil?
        path << "?window=#{@window}"
      elsif !@sys.nil?
        path << "?sys=#{@sys}"
      end
    end

    def parse_json(json_response)
      JSON.parse(json_response)
    rescue => e
      @log.error("Unable to parse response body #{json_response}",
                 error: e.to_s, error_class: e.class.to_s)
    end

    def do_request(uri)
      @log.debug("Starting request to #{uri}")
      Net::HTTP.start(uri.host, uri.port,
                      use_ssl: uri.scheme == 'https') do |http|
        request = Net::HTTP::Get.new(uri.request_uri)

        if !@user.nil? && !@password.nil?
          @log.debug("Using basic auth with user: #{@user}")
          request.basic_auth @user, @password
        end

        return http.request(request)
      end
    end

    def to_uri(base, path)
      URI.parse("#{base}#{path}")
    end

    class TimerWatcher < Coolio::TimerWatcher
      def initialize(interval, repeat, log, &callback)
        @log = log
        @callback = callback
        super(interval, repeat)
      end

      def on_timer
        @callback.call
      rescue => e
        @log.error e.to_s
        @log.error_backtrace
      end
    end
  end
end
