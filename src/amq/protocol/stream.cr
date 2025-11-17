require "./errors"
require "./frames"

module AMQ
  module Protocol
    class Stream < IO
      @io : IO
      @frame_max : UInt32
      @frame_size : UInt32
      @format : IO::ByteFormat

      def initialize(@io : IO, @frame_max = 8192_u32, @format = IO::ByteFormat::NetworkEndian)
        @frame_size = 0_u32
      end

      protected def assert_within_frame(size : Int) : Nil
        if size > @frame_size
          raise IO::Error.new("Cannot allocate #{size} bytes, only #{@frame_size} bytes left in frame size limit")
        end
      end

      def read(slice : Bytes)
        if slice.size > @frame_size
          raise IO::Error.new("Stream has reached frame size limit")
        end
        count = @io.read(slice)
        if count > @frame_size
          raise IO::Error.new("Stream has reached frame size limit")
        end
        @frame_size -= count
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
          result = yield frame
          read_frame_end
          result
        rescue ex
          begin
            @io.read_byte
          rescue IO::Error
          end
          raise ex
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
            @io.read_fully bytes
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
        size = @format.decode(UInt32, slice[3, 4])
        if size > @frame_max
          raise IO::Error.new("Frame size #{size} exceeds max frame size #{@frame_max}")
          # Doesn't take into account the frame overhead (8 bytes)
        end
        @frame_size = size
        return type, channel, size
      end

      private def read_frame_end : Nil
        if (frame_end = @io.read_byte) && frame_end != 206_u8
          raise Protocol::Error::InvalidFrameEnd.new("Frame-end was #{frame_end}, expected 206")
        end
      end
    end
  end
end
