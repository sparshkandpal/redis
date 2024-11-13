# frozen_string_literal: true

require 'socket'

class YourRedisServer
  def initialize(port)
    @port = port
  end

  def start
    server = TCPServer.new(@port)
    @clients = []

    loop do
      # Add server and clients to watch list
      fds_to_watch = [server, *@clients]
      ready_to_read, _, _ = IO.select(fds_to_watch)

      ready_to_read.each do |ready|
        if ready == server
          # Accept new client and add to clients list
          @clients << server.accept
        else
          # Handle client request
          handle_client(ready)
        end
      end
    end
  end

  def handle_client(client)
    request = client.readpartial(1024)

    puts("req #{request}")

    if request.start_with?("*1\r\n$4\r\nPING\r\n")
      client.write("+PONG\r\n")
    elsif request.start_with?("*2\r\n$4\r\nECHO\r\n")
      message = request[18..].strip 
      response = "$#{message.bytesize}\r\n#{message}\r\n"
      client.write(response)
    end

  rescue EOFError
    # If client disconnected, remove it from the clients list and close the socket
    @clients.delete(client)
    client.close
  end

  def echo(request)

  end
end

YourRedisServer.new(6379).start
