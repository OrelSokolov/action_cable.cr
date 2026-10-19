# CorkWriter: per-connection outgoing buffer flushed as one write —
# the uWS cork/drain analog (P1.5, see PLAN).
#
# Producers append complete frames under a short lock; a writer fiber
# swaps the buffer out and issues a single write+flush per batch. While
# the batch is in flight new frames accumulate in the next batch. The
# buffer is capped; overflow drops the message and counts it (Bun's
# DROPPED semantics) so a slow client never stalls the fan-out sweep.
#
# The same IO lock must wrap any other writer of the socket (stdlib
# auto-pongs) via LockedImageIO so frames never interleave.

module ActionCable
  # Wraps the upgraded socket handed to HTTP::WebSocket so its own
  # writes (auto-pong from the reader fiber) serialize with CorkWriter
  # batch writes at frame granularity.
  class LockedImageIO < IO
    def initialize(@io : IO, @lock : Mutex)
    end

    def read(slice : Bytes) : Int32
      @io.read(slice)
    end

    def write(slice : Bytes) : Nil
      @lock.synchronize { @io.write(slice) }
    end

    def flush : Nil
      @io.flush
    end

    def close : Nil
      @io.close
    end
  end

  class CorkWriter
    MAX_BUFFER = 1_048_576 # cork cap; overflow drops (Bun DROPPED analog)

    attr_reader dropped_messages : Int32 = 0

    def initialize(@io : IO, @iolock : Mutex)
      @wlock = Mutex.new
      @wbuf = IO::Memory.new(capacity: 8192)
      @wake = ::Channel(Nil).new(1)
      @closed = false
      @dead = false
      spawn_writer
    end

    # Frames and buffers one text message.
    def deliver_text(text : String) : Nil
      return if @closed
      len = text.bytesize
      @wlock.synchronize do
        if @wbuf.bytesize + len + 10 > MAX_BUFFER
          @dropped_messages += 1
          return
        end
        CorkWriter.frame_header(@wbuf, len)
        @wbuf.write(text.to_slice)
      end
      wake_writer
    end

    # Buffers an already-encoded frame (see .text_frame) — encode-once
    # fan-out: the same bytes go to every socket.
    def deliver_frame(frame : Bytes) : Nil
      return if @closed
      @wlock.synchronize do
        if @wbuf.bytesize + frame.size > MAX_BUFFER
          @dropped_messages += 1
          return
        end
        @wbuf.write(frame)
      end
      wake_writer
    end

    # Builds a complete outgoing text frame. Server→client frames are
    # unmasked, so one pre-encoded frame can be written to every socket
    # verbatim.
    def self.text_frame(text : String) : Bytes
      header = IO::Memory.new(10)
      frame_header(header, text.bytesize)
      frame = Bytes.new(header.size + text.bytesize)
      header.to_slice.copy_to(frame)
      text.to_slice.copy_to(frame + header.size)
      frame
    end

    def buffered_amount : Int32
      @wlock.synchronize { @wbuf.bytesize.to_i32 }
    end

    # Writes out buffered frames synchronously — used before a WS close
    # handshake (remote disconnect) so the disconnect frame precedes the
    # close frame on the wire.
    def flush : Nil
      while @wlock.synchronize { @wbuf.bytesize } > 0
        return if @dead # writer fiber gave up on a dead socket
        wake_writer
        Fiber.yield
      end
    end

    # Drains what is left and stops the writer fiber.
    def close : Nil
      @closed = true
      wake_writer
    end

    protected def self.frame_header(buf : IO::Memory, len : Int) : Nil
      buf.write_byte(0x81_u8) # FIN + text opcode; server frames unmasked
      if len < 126
        buf.write_byte(len.to_u8)
      elsif len <= 65535
        buf.write_byte(126_u8)
        buf.write_bytes(len.to_u16, IO::ByteFormat::BigEndian)
      else
        buf.write_byte(127_u8)
        buf.write_bytes(len.to_u64, IO::ByteFormat::BigEndian)
      end
    end

    private def wake_writer : Nil
      select
      when @wake.send(nil)
      else # token already pending — the writer will pick our bytes up
      end
    end

    private def spawn_writer : Nil
      spawn do
        loop do
          @wake.receive
          break if @closed && @wbuf.bytesize == 0

          batch = @wlock.synchronize do
            size = @wbuf.bytesize
            next Bytes.empty if size == 0
            data = @wbuf.to_slice[0, size].dup
            @wbuf.clear
            data
          end

          unless batch.empty?
            begin
              # Sockets are IO::Buffered — flush pushes the batch out as
              # one write(2).
              @iolock.synchronize do
                @io.write(batch)
                @io.flush
              end
            rescue IO::Error
              @dead = true
              break # dead socket; the reader fiber observes EOF and closes
            end
          end
          break if @closed && @wbuf.bytesize == 0
        end
      end
    end
  end
end

require "http/web_socket"
