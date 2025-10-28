require "./errors"
require "./frames"

module AMQ
  module Protocol
    # An IO wrapper that enforces AMQP frame size limits and reads frames
    # Use `next_frame` to read the next frame from the stream
    # Don't use `read` directly unless you know what you're doing
    class Stream < IO
      @io : IO
      @frame_max : UInt32
      @frame_remaining = 0_u32
      @format : IO::ByteFormat

      # Construct a new Stream wrapping the given `io`
      # `frame_max` applies to payload size only,
      # excluding the 8-byte frame envelope (7-byte header + 1-byte end marker)
      def initialize(@io : IO, @frame_max = 8192_u32, @format = IO::ByteFormat::NetworkEndian)
      end

      protected def assert_within_frame(size : Int) : Nil
        if size > @frame_remaining
          raise Protocol::Error::FrameSizeError.new("Cannot allocate #{size} bytes, only #{@frame_remaining} bytes left in frame size limit")
        end
      end

      def read(slice : Bytes)
        if slice.size > @frame_remaining
          raise Protocol::Error::FrameSizeError.new("Frame has reached frame size limit")
        end
        count = @io.read(slice)
        @frame_remaining -= count
        count
      end

      # Delegate write to underlying IO without frame size checks
      def write(slice : Bytes) : Nil
        @io.write(slice)
      end

      def flush
        @io.flush
      end

      def next_frame(& : Frame -> _)
        type, channel, size = read_frame_header

        frame =
          case type
          when Frame::Method::TYPE
            Frame::Method.from_io(channel, size, self, @format)
          when Frame::Header::TYPE
            Frame::Header.from_io(channel, size, self, @format)
          when Frame::Body::TYPE
            Frame::Body.new(channel, size, self)
          when Frame::Heartbeat::TYPE
            Frame::Heartbeat.from_io(channel, size, self, @format)
          else
            raise Protocol::Error::FrameDecode.new("Invalid frame type #{type}")
          end

        begin
          yield frame
        ensure
          read_frame_end
        end
      end

      def next_frame : Frame
        type, channel, size = read_frame_header

        frame =
          case type
          when Frame::Method::TYPE
            Frame::Method.from_io(channel, size, self, @format)
          when Frame::Header::TYPE
            Frame::Header.from_io(channel, size, self, @format)
          when Frame::Body::TYPE
            assert_within_frame(size)
            bytes = Bytes.new(size)
            read_fully bytes
            Frame::BytesBody.new(channel, size, bytes)
          when Frame::Heartbeat::TYPE
            Frame::Heartbeat.from_io(channel, size, self, @format)
          else
            raise Protocol::Error::FrameDecode.new("Invalid frame type #{type}")
          end

        read_frame_end
        frame
      end

      private def read_frame_header : Tuple(UInt8, UInt16, UInt32)
        buf = uninitialized UInt8[7]
        slice = buf.to_slice
        @io.read_fully(slice)
        type = slice[0]
        channel = @format.decode(UInt16, slice[1, 2])
        frame_size = @format.decode(UInt32, slice[3, 4])
        if frame_size > @frame_max
          raise Protocol::Error::FrameSizeError.new("Frame size #{frame_size} exceeds max frame size #{@frame_max}")
          # Doesn't take into account the frame overhead (8 bytes)
        end
        @frame_remaining = frame_size
        return type, channel, frame_size
      end

      private def read_frame_end : Nil
        if frame_end = @io.read_byte
          if frame_end != 206_u8
            raise Protocol::Error::InvalidFrameEnd.new("Frame-end was #{frame_end}, expected 206")
          end
        else
          raise IO::EOFError.new("Unexpected EOF while reading frame-end")
        end
        if @frame_remaining != 0
          raise Protocol::Error::FrameSizeError.new("Frame not fully read, #{@frame_remaining} bytes remaining")
        end
      end
    end
  end
end
