# frozen_string_literal: true

require 'socket'

class YourRedisServer
  def initialize(port)
    @port = port
  end

  def start
    server = TCPServer.new(@port)
    client = server.accept

    loop do
      # Read data from client
      request = client.gets

      case request.strip
      when 'PING'
        client.puts("+PONG\r\n")
      end
    end
  end
end

YourRedisServer.new(6379).start
