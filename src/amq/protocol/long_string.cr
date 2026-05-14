module AMQ
  module Protocol
    struct LongString
      def initialize(@str : String)
      end

      def to_io(io, format) : Nil
        raise ArgumentError.new("Long string is too long, max #{UInt32::MAX}") if @str.bytesize > UInt32::MAX
        io.write_bytes(@str.bytesize.to_u32, format)
        io.write(@str.to_slice)
      end

      def self.from_io(io, format) : String
        sz = UInt32.from_io(io, format)
        # Check size limit before allocation if IO supports it
        if stream = io.as?(AMQ::Protocol::Stream)
          stream.assert_within_frame(sz)
        end
        io.read_string(sz)
      end
    end
  end
end
