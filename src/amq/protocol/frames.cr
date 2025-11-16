module AMQ
  module Protocol
    abstract struct Frame
      getter channel, bytesize

      def initialize(@channel : UInt16, @bytesize : UInt32)
      end

      abstract def to_io(io : IO, format : IO::ByteFormat)
      abstract def type : UInt8

      def wrap(io, format : IO::ByteFormat, &) : Nil
        buf = uninitialized UInt8[7]
        buf[0] = type
        slice = buf.to_slice
        format.encode @channel, slice[1, 2]
        format.encode @bytesize, slice[3, 4]
        io.write slice
        yield
        io.write_byte 206_u8
      end

      # Parse a frame from an IO
      #
      # Requires a block, because the Body is not buffered and can instead be streamed efficiently.
      def self.from_io(io, format = IO::ByteFormat::NetworkEndian, & : Frame -> _)
        buf = uninitialized UInt8[7]
        slice = buf.to_slice
        io.read_fully(slice)
        type = slice[0]
        channel = format.decode(UInt16, slice[1, 2])
        size = format.decode(UInt32, slice[3, 4])
        frame =
          case type
          when Method::TYPE    then Method.from_io(channel, size, io, format)
          when Header::TYPE    then Header.from_io(channel, size, io, format)
          when Body::TYPE      then Body.new(channel, size, io)
          when Heartbeat::TYPE then Heartbeat.from_io(channel, size, io, format)
          else
            raise Error::FrameDecode.new("Invalid frame type #{type}")
          end

        begin
          result = yield frame
          if (frame_end = io.read_byte) && frame_end != 206_u8
            raise Error::InvalidFrameEnd.new("#{frame.class}-end was #{frame_end}, expected 206")
          end
          result
        rescue ex
          begin
            io.read_byte
          rescue IO::Error
          end
          raise ex
        end
      end

      # Parse a frame from an IO
      #
      # Note that this method buffers `BytesBody` frames,
      # only use this method if you don't require the best performance.
      def self.from_io(io, format = IO::ByteFormat::NetworkEndian)
        buf = uninitialized UInt8[7]
        slice = buf.to_slice
        io.read_fully(slice)
        type = slice[0]
        channel = format.decode(UInt16, slice[1, 2])
        size = format.decode(UInt32, slice[3, 4])
        frame =
          case type
          when Method::TYPE then Method.from_io(channel, size, io, format)
          when Header::TYPE then Header.from_io(channel, size, io, format)
          when Body::TYPE
            bytes = Bytes.new(size)
            io.read_fully bytes
            BytesBody.new(channel, size, bytes)
          when Heartbeat::TYPE then Heartbeat.from_io(channel, size, io, format)
          else
            raise Error::FrameDecode.new("Invalid frame type #{type}")
          end

        if (frame_end = io.read_byte) && frame_end != 206_u8
          raise Error::InvalidFrameEnd.new("#{frame.class}-end was #{frame_end}, expected 206")
        end
        frame
      end

      def to_slice(format = IO::ByteFormat::SystemEndian) : Bytes
        io = IO::Memory.new(bytesize)
        to_io(io, format)
        io.to_slice
      end

      # Helper method to calculate ShortString size (1 byte length + content)
      private def short_string_size(str : String) : UInt32
        sizeof(UInt8).to_u32 + str.bytesize
      end

      # Helper method to calculate LongString size (4 bytes length + content)
      private def long_string_size(str : String) : UInt32
        sizeof(UInt32).to_u32 + str.bytesize
      end

      struct Header < Frame
        TYPE = 2_u8

        def type : UInt8
          TYPE
        end

        getter class_id, weight, body_size, properties

        def initialize(channel : UInt16, @class_id : UInt16, @weight : UInt16, @body_size : UInt64,
                       @properties : Properties, bytesize : UInt32? = nil)
          if bytesize.nil?
            bytesize = sizeof(UInt16) +            # class_id (2 bytes)
                       sizeof(UInt16) +            # weight (2 bytes)
                       sizeof(UInt64) +            # body_size (8 bytes)
                       @properties.bytesize.to_u32 # properties
          end
          super(channel, bytesize.to_u32)
        end

        def to_io(io : IO, format : IO::ByteFormat)
          wrap(io, format) do
            buf = uninitialized UInt8[12]
            slice = buf.to_slice
            format.encode @class_id, slice[0, 2]
            format.encode @weight, slice[2, 2]
            format.encode @body_size, slice[4, 8]
            io.write slice
            io.write_bytes @properties, format
          end
        end

        def self.from_io(channel, bytesize, io, format)
          buf = uninitialized UInt8[14]
          slice = buf.to_slice
          io.read_fully(slice)
          class_id = format.decode(UInt16, slice[0, 2])
          weight = format.decode(UInt16, slice[2, 2])
          body_size = format.decode(UInt64, slice[4, 8])
          property_flags = format.decode(UInt16, slice[12, 2])
          props = Properties.from_io(io, format, property_flags)
          new channel, class_id, weight, body_size, props, bytesize
        end
      end

      struct Body < Frame
        TYPE = 3_u8

        def type : UInt8
          TYPE
        end

        getter body_size, body

        def initialize(channel : UInt16, @body_size : UInt32, @body : IO)
          super(channel, @body_size)
        end

        def to_io(io, format)
          wrap(io, format) do
            copied = IO.copy(@body, io, @body_size)
            if copied != @body_size
              raise Error::FrameEncode.new("Only #{copied} bytes of #{@body_size} of the body could be copied")
            end
          end
        end
      end

      struct BytesBody < Frame
        TYPE = 3_u8

        def type : UInt8
          TYPE
        end

        getter body_size, body

        def initialize(channel : UInt16, @body_size : UInt32, @body : Bytes)
          super(channel, @body_size)
        end

        def to_io(io, format)
          wrap(io, format) do
            io.write @body
          end
        end
      end

      struct Heartbeat < Frame
        TYPE = 8_u8

        def type : UInt8
          TYPE
        end

        def initialize
          @channel = 0_u16
          @bytesize = 0_u32
        end

        def to_io(io, format)
          wrap(io, format) { }
        end

        def self.from_io(channel, size, io, format)
          unless channel.zero?
            raise Protocol::Error::FrameDecode.new("Heartbeat frame channel must be 0, got #{channel}")
          end
          unless size.zero?
            raise Protocol::Error::FrameDecode.new("Heartbeat frame size must be 0, got #{size}")
          end
          new
        end
      end

      alias MessageFrame = Body | Header | Method::Basic::Publish

      abstract struct Method < Frame
        TYPE = 1_u8

        def type : UInt8
          TYPE
        end

        def initialize(channel : UInt16, bytesize : UInt32 = 0_u32)
          # Method frames have a 2-byte class_id and 2-byte method_id
          super(channel, bytesize + sizeof(UInt16) + sizeof(UInt16))
        end

        abstract def class_id : UInt16
        abstract def method_id : UInt16

        def wrap(io, format, &)
          super(io, format) do
            buf = uninitialized UInt8[4]
            slice = buf.to_slice
            format.encode(class_id, slice[0, 2])
            format.encode(method_id, slice[2, 2])
            io.write slice
            yield
          end
        end

        # ameba:disable Metrics/CyclomaticComplexity
        def self.from_io(channel, bytesize, io, format)
          buf = uninitialized UInt8[4]
          slice = buf.to_slice
          io.read_fully(slice)
          class_id = format.decode(UInt16, slice[0, 2])
          method_id = format.decode(UInt16, slice[2, 2])
          bytesize -= (sizeof(UInt16) + sizeof(UInt16)) # (class_id + method_id)
          case class_id
          when 10_u16
            case method_id
            when 10_u16 then Connection::Start.from_io(io, bytesize, format)
            when 11_u16 then Connection::StartOk.from_io(io, bytesize, format)
            when 30_u16 then Connection::Tune.from_io(io, bytesize, format)
            when 31_u16 then Connection::TuneOk.from_io(io, bytesize, format)
            when 40_u16 then Connection::Open.from_io(io, bytesize, format)
            when 41_u16 then Connection::OpenOk.from_io(io, bytesize, format)
            when 50_u16 then Connection::Close.from_io(io, bytesize, format)
            when 51_u16 then Connection::CloseOk.from_io(io, bytesize, format)
            when 60_u16 then Connection::Blocked.from_io(io, bytesize, format)
            when 61_u16 then Connection::Unblocked.from_io(io, bytesize, format)
            when 70_u16 then Connection::UpdateSecret.from_io(io, bytesize, format)
            when 71_u16 then Connection::UpdateSecretOk.from_io(io, bytesize, format)
            else             raise Error::NotImplemented.new(channel, class_id, method_id)
            end
          when 20_u16
            case method_id
            when 10_u16 then Channel::Open.from_io(channel, bytesize, io, format)
            when 11_u16 then Channel::OpenOk.from_io(channel, bytesize, io, format)
            when 20_u16 then Channel::Flow.from_io(channel, bytesize, io, format)
            when 21_u16 then Channel::FlowOk.from_io(channel, bytesize, io, format)
            when 40_u16 then Channel::Close.from_io(channel, bytesize, io, format)
            when 41_u16 then Channel::CloseOk.from_io(channel, bytesize, io, format)
            else             raise Error::NotImplemented.new(channel, class_id, method_id)
            end
          when 40_u16
            case method_id
            when 10_u16 then Exchange::Declare.from_io(channel, bytesize, io, format)
            when 11_u16 then Exchange::DeclareOk.from_io(channel, bytesize, io, format)
            when 20_u16 then Exchange::Delete.from_io(channel, bytesize, io, format)
            when 21_u16 then Exchange::DeleteOk.from_io(channel, bytesize, io, format)
            when 30_u16 then Exchange::Bind.from_io(channel, bytesize, io, format)
            when 31_u16 then Exchange::BindOk.from_io(channel, bytesize, io, format)
            when 40_u16 then Exchange::Unbind.from_io(channel, bytesize, io, format)
            when 51_u16 then Exchange::UnbindOk.from_io(channel, bytesize, io, format)
            else             raise Error::NotImplemented.new(channel, class_id, method_id)
            end
          when 50_u16
            case method_id
            when 10_u16 then Queue::Declare.from_io(channel, bytesize, io, format)
            when 11_u16 then Queue::DeclareOk.from_io(channel, bytesize, io, format)
            when 20_u16 then Queue::Bind.from_io(channel, bytesize, io, format)
            when 21_u16 then Queue::BindOk.from_io(channel, bytesize, io, format)
            when 30_u16 then Queue::Purge.from_io(channel, bytesize, io, format)
            when 31_u16 then Queue::PurgeOk.from_io(channel, bytesize, io, format)
            when 40_u16 then Queue::Delete.from_io(channel, bytesize, io, format)
            when 41_u16 then Queue::DeleteOk.from_io(channel, bytesize, io, format)
            when 50_u16 then Queue::Unbind.from_io(channel, bytesize, io, format)
            when 51_u16 then Queue::UnbindOk.from_io(channel, bytesize, io, format)
            else             raise Error::NotImplemented.new(channel, class_id, method_id)
            end
          when 60_u16
            case method_id
            when  10_u16 then Basic::Qos.from_io(channel, bytesize, io, format)
            when  11_u16 then Basic::QosOk.from_io(channel, bytesize, io, format)
            when  20_u16 then Basic::Consume.from_io(channel, bytesize, io, format)
            when  21_u16 then Basic::ConsumeOk.from_io(channel, bytesize, io, format)
            when  30_u16 then Basic::Cancel.from_io(channel, bytesize, io, format)
            when  31_u16 then Basic::CancelOk.from_io(channel, bytesize, io, format)
            when  40_u16 then Basic::Publish.from_io(channel, bytesize, io, format)
            when  50_u16 then Basic::Return.from_io(channel, bytesize, io, format)
            when  60_u16 then Basic::Deliver.from_io(channel, bytesize, io, format)
            when  70_u16 then Basic::Get.from_io(channel, bytesize, io, format)
            when  71_u16 then Basic::GetOk.from_io(channel, bytesize, io, format)
            when  72_u16 then Basic::GetEmpty.from_io(channel, bytesize, io, format)
            when  80_u16 then Basic::Ack.from_io(channel, bytesize, io, format)
            when  90_u16 then Basic::Reject.from_io(channel, bytesize, io, format)
            when 110_u16 then Basic::Recover.from_io(channel, bytesize, io, format)
            when 111_u16 then Basic::RecoverOk.from_io(channel, bytesize, io, format)
            when 120_u16 then Basic::Nack.from_io(channel, bytesize, io, format)
            else              raise Error::NotImplemented.new(channel, class_id, method_id)
            end
          when 85_u16
            case method_id
            when 10_u16 then Confirm::Select.from_io(channel, bytesize, io, format)
            when 11_u16 then Confirm::SelectOk.from_io(channel, bytesize, io, format)
            else             raise Error::NotImplemented.new(channel, class_id, method_id)
            end
          when 90_u16
            case method_id
            when 10_u16 then Tx::Select.from_io(channel, bytesize, io, format)
            when 11_u16 then Tx::SelectOk.from_io(channel, bytesize, io, format)
            when 20_u16 then Tx::Commit.from_io(channel, bytesize, io, format)
            when 21_u16 then Tx::CommitOk.from_io(channel, bytesize, io, format)
            when 30_u16 then Tx::Rollback.from_io(channel, bytesize, io, format)
            when 31_u16 then Tx::RollbackOk.from_io(channel, bytesize, io, format)
            else             raise Error::NotImplemented.new(channel, class_id, method_id)
            end
          else
            raise Error::NotImplemented.new(channel, class_id, method_id)
          end
        end
      end

      abstract struct Connection < Method
        CLASS_ID = 10_u16

        def class_id : UInt16
          CLASS_ID
        end

        def initialize(bytesize : UInt32 = 0_u32)
          super(0_u16, bytesize)
        end

        struct Start < Connection
          METHOD_ID = 10_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_byte(@version_major)
              io.write_byte(@version_minor)
              io.write_bytes @server_properties, format
              io.write_bytes LongString.new(@mechanisms), format
              io.write_bytes LongString.new(@locales), format
            end
          end

          getter server_properties

          def initialize(@version_major = 0_u8, @version_minor = 9_u8,
                         @server_properties = Table.new({
                           capabilities: {
                             "publisher_confirms":           true,
                             "exchange_exchange_bindings":   true,
                             "basic.nack":                   true,
                             "per_consumer_qos":             true,
                             "authentication_failure_close": true,
                             "consumer_cancel_notify":       true,
                             "connection.blocked":           true,
                           },
                         }),
                         @mechanisms = "AMQPLAIN PLAIN",
                         @locales = "en_US",
                         bytesize : UInt32? = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt8) +                      # version_major (1 byte)
                         sizeof(UInt8) +                      # version_minor (1 byte)
                         @server_properties.bytesize.to_u32 + # server_properties Table
                         long_string_size(@mechanisms) +      # mechanisms LongString (4 + length)
                         long_string_size(@locales)           # locales LongString (4 + length)
            end
            super(bytesize.to_u32)
          end

          def self.from_io(io, bytesize, format)
            version_major = io.read_byte || raise IO::EOFError.new
            version_minor = io.read_byte || raise IO::EOFError.new
            server_properties = Table.from_io(io, format)
            mech = LongString.from_io(io, format)
            locales = LongString.from_io(io, format)
            new(version_major, version_minor, server_properties, mech, locales, bytesize)
          end
        end

        struct StartOk < Connection
          getter client_properties, mechanism, response, locale

          METHOD_ID = 11_u16

          def method_id : UInt16
            METHOD_ID
          end

          def initialize(@client_properties : Table, @mechanism : String,
                         @response : String, @locale : String, bytesize = nil)
            if bytesize.nil?
              bytesize = @client_properties.bytesize.to_u32 + # client_properties Table
                         short_string_size(@mechanism) +      # mechanism ShortString (1 + length)
                         long_string_size(@response) +        # response LongString (4 + length)
                         short_string_size(@locale)           # locale ShortString (1 + length)
            end
            super(bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @client_properties, format
              io.write_bytes ShortString.new(@mechanism), format
              io.write_bytes LongString.new(@response), format
              io.write_bytes ShortString.new(@locale), format
            end
          end

          def self.from_io(io, bytesize, format)
            props = Table.from_io(io, format)
            mech = ShortString.from_io(io, format)
            auth = LongString.from_io(io, format)
            locale = ShortString.from_io(io, format)
            new(props, mech, auth, locale, bytesize)
          end
        end

        struct Tune < Connection
          getter channel_max, frame_max, heartbeat
          METHOD_ID = 30_u16

          def method_id : UInt16
            METHOD_ID
          end

          def initialize(@channel_max = 0_u16, @frame_max = 131072_u32, @heartbeat = 0_u16)
            super(8_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@channel_max, format)
              io.write_bytes(@frame_max, format)
              io.write_bytes(@heartbeat, format)
            end
          end

          def self.from_io(io, bytesize, format)
            channel_max = UInt16.from_io(io, format)
            frame_max = UInt32.from_io(io, format)
            heartbeat = UInt16.from_io(io, format)
            new(channel_max, frame_max, heartbeat)
          end
        end

        struct TuneOk < Connection
          getter channel_max, frame_max, heartbeat
          METHOD_ID = 31_u16

          def method_id : UInt16
            METHOD_ID
          end

          def initialize(@channel_max = 0_u16, @frame_max = 131072_u32, @heartbeat = 60_u16)
            super(8_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@channel_max, format)
              io.write_bytes(@frame_max, format)
              io.write_bytes(@heartbeat, format)
            end
          end

          def self.from_io(io, bytesize, format)
            channel_max = UInt16.from_io(io, format)
            frame_max = UInt32.from_io(io, format)
            heartbeat = UInt16.from_io(io, format)
            new(channel_max, frame_max, heartbeat)
          end
        end

        struct Open < Connection
          getter vhost, reserved1, reserved2
          METHOD_ID = 40_u16

          def method_id : UInt16
            METHOD_ID
          end

          def initialize(@vhost = "/", @reserved1 = "", @reserved2 = false, bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@vhost) +     # vhost ShortString
                         short_string_size(@reserved1) + # reserved1 ShortString
                         sizeof(Bool)                    # reserved2 boolean
            end
            super(bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@vhost), format
              io.write_bytes ShortString.new(@reserved1), format
              io.write_byte @reserved2 ? 1_u8 : 0_u8
            end
          end

          def self.from_io(io, bytesize, format)
            vhost = ShortString.from_io(io, format)
            reserved1 = ShortString.from_io(io, format)
            reserved2 = (io.read_byte || raise IO::EOFError.new) > 0
            new(vhost, reserved1, reserved2, bytesize)
          end
        end

        struct OpenOk < Connection
          getter reserved1

          METHOD_ID = 41_u16

          def method_id : UInt16
            METHOD_ID
          end

          def initialize(@reserved1 = "", bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@reserved1) # reserved1 ShortString
            end
            super(bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@reserved1), format
            end
          end

          def self.from_io(io, bytesize, format)
            reserved1 = ShortString.from_io(io, format)
            new(reserved1, bytesize)
          end
        end

        struct Close < Connection
          getter reply_code, reply_text, failing_class_id, failing_method_id

          METHOD_ID = 50_u16

          def method_id : UInt16
            METHOD_ID
          end

          def initialize(@reply_code : UInt16, @reply_text : String, @failing_class_id : UInt16,
                         @failing_method_id : UInt16, bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                 # reply_code
                         short_string_size(@reply_text) + # reply_text ShortString
                         sizeof(UInt16) +                 # failing_class_id
                         sizeof(UInt16)                   # failing_method_id
            end
            super(bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@reply_code, format)
              io.write_bytes ShortString.new(@reply_text), format
              io.write_bytes(@failing_class_id, format)
              io.write_bytes(@failing_method_id, format)
            end
          end

          def self.from_io(io, bytesize, format)
            code = UInt16.from_io(io, format)
            text = ShortString.from_io(io, format)
            failing_class_id = UInt16.from_io(io, format)
            failing_method_id = UInt16.from_io(io, format)
            new(code, text, failing_class_id, failing_method_id, bytesize)
          end
        end

        struct CloseOk < Connection
          METHOD_ID = 51_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(io, bytesize, format)
            new
          end
        end

        struct Blocked < Connection
          METHOD_ID = 60_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reason

          def initialize(@reason : String, bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@reason) # reason ShortString
            end
            super(bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@reason), format
            end
          end

          def self.from_io(io, bytesize, format)
            reason = ShortString.from_io(io, format)
            new(reason, bytesize)
          end
        end

        struct Unblocked < Connection
          METHOD_ID = 61_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(io, bytesize, format)
            new
          end
        end

        struct UpdateSecret < Connection
          METHOD_ID = 70_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter secret, reason

          def initialize(@secret : String, @reason : String, bytesize = nil)
            if bytesize.nil?
              bytesize = long_string_size(@secret) + # secret LongString
                         short_string_size(@reason)  # reason ShortString
            end
            super(bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes LongString.new(@secret), format
              io.write_bytes ShortString.new(@reason), format
            end
          end

          def self.from_io(io, bytesize, format)
            secret = LongString.from_io(io, format)
            reason = ShortString.from_io(io, format)
            new(secret, reason, bytesize)
          end
        end

        struct UpdateSecretOk < Connection
          METHOD_ID = 71_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(io, bytesize, format)
            new
          end
        end
      end

      abstract struct Channel < Method
        CLASS_ID = 20_u16

        def class_id : UInt16
          CLASS_ID
        end

        struct Open < Channel
          METHOD_ID = 10_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1

          def initialize(channel : UInt16, @reserved1 = "", bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@reserved1) # reserved1 ShortString
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@reserved1), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = ShortString.from_io(io, format)
            new channel, reserved1, bytesize
          end
        end

        struct OpenOk < Channel
          METHOD_ID = 11_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1

          def initialize(channel : UInt16, @reserved1 = "", bytesize = nil)
            if bytesize.nil?
              bytesize = long_string_size(@reserved1) # reserved1 LongString
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes LongString.new(@reserved1), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = LongString.from_io(io, format)
            new channel, reserved1, bytesize
          end
        end

        struct Flow < Channel
          METHOD_ID = 20_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter active

          def initialize(channel : UInt16, @active : Bool)
            super(channel, 1_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_byte @active ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            active = (io.read_byte || raise IO::EOFError.new) > 0
            new channel, active
          end
        end

        struct FlowOk < Channel
          METHOD_ID = 21_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter active

          def initialize(channel : UInt16, @active : Bool)
            super(channel, 1_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_byte @active ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            active = (io.read_byte || raise IO::EOFError.new) > 0
            new channel, active
          end
        end

        struct Close < Channel
          METHOD_ID = 40_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reply_code, reply_text, classid, methodid

          def initialize(channel : UInt16, @reply_code : UInt16, @reply_text : String,
                         @classid : UInt16, @methodid : UInt16, bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                 # reply_code
                         short_string_size(@reply_text) + # reply_text ShortString
                         sizeof(UInt16) +                 # classid
                         sizeof(UInt16)                   # methodid
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@reply_code, format)
              io.write_bytes ShortString.new(@reply_text), format
              io.write_bytes(@classid, format)
              io.write_bytes(@methodid, format)
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reply_code = UInt16.from_io(io, format)
            reply_text = ShortString.from_io(io, format)
            classid = UInt16.from_io(io, format)
            methodid = UInt16.from_io(io, format)
            new channel, reply_code, reply_text, classid, methodid, bytesize
          end
        end

        struct CloseOk < Channel
          METHOD_ID = 41_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new channel
          end
        end
      end

      abstract struct Exchange < Method
        CLASS_ID = 40_u16

        def class_id : UInt16
          CLASS_ID
        end

        struct Declare < Exchange
          METHOD_ID = 10_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, exchange_name, exchange_type, passive, durable, auto_delete, internal, no_wait, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @exchange_name : String,
                         @exchange_type : String, @passive : Bool, @durable : Bool, @auto_delete : Bool,
                         @internal : Bool, @no_wait : Bool, @arguments : Table,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                    # reserved1
                         short_string_size(@exchange_name) + # exchange_name ShortString
                         short_string_size(@exchange_type) + # exchange_type ShortString
                         sizeof(Bool) +                      # bit field
                         @arguments.bytesize.to_u32          # arguments Table
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@exchange_name), format
              io.write_bytes ShortString.new(@exchange_type), format
              bits = 0_u8
              bits = bits | (1 << 0) if @passive
              bits = bits | (1 << 1) if @durable
              bits = bits | (1 << 2) if @auto_delete
              bits = bits | (1 << 3) if @internal
              bits = bits | (1 << 4) if @no_wait
              io.write_byte(bits)
              io.write_bytes @arguments, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            name = ShortString.from_io(io, format)
            type = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            passive = bits.bit(0) == 1
            durable = bits.bit(1) == 1
            auto_delete = bits.bit(2) == 1
            internal = bits.bit(3) == 1
            no_wait = bits.bit(4) == 1
            args = Table.from_io(io, format)

            new channel, reserved1, name, type, passive, durable, auto_delete, internal, no_wait, args, bytesize
          end
        end

        struct DeclareOk < Exchange
          METHOD_ID = 11_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct Delete < Exchange
          METHOD_ID = 20_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, exchange_name, if_unused, no_wait

          def initialize(channel : UInt16, @reserved1 : UInt16, @exchange_name : String,
                         @if_unused : Bool, @no_wait : Bool, bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                    # reserved1
                         short_string_size(@exchange_name) + # exchange_name ShortString
                         sizeof(Bool)                        # bit field
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@exchange_name), format
              bits = 0_u8
              bits = bits | (1 << 0) if @if_unused
              bits = bits | (1 << 1) if @no_wait
              io.write_byte(bits)
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            name = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            if_unused = bits.bit(0) == 1
            no_wait = bits.bit(1) == 1
            new channel, reserved1, name, if_unused, no_wait, bytesize
          end
        end

        struct DeleteOk < Exchange
          METHOD_ID = 21_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct Bind < Exchange
          METHOD_ID = 30_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, destination, source, routing_key, no_wait, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @destination : String,
                         @source : String, @routing_key : String, @no_wait : Bool,
                         @arguments : Table,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                  # reserved1 (2 bytes)
                         short_string_size(@destination) + # destination ShortString (1 + length)
                         short_string_size(@source) +      # source ShortString (1 + length)
                         short_string_size(@routing_key) + # routing_key ShortString (1 + length)
                         sizeof(Bool) +                    # no_wait boolean (1 byte)
                         @arguments.bytesize.to_u32        # arguments Table
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@destination), format
              io.write_bytes ShortString.new(@source), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_byte @no_wait ? 1_u8 : 0_u8
              io.write_bytes @arguments, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            destination = ShortString.from_io(io, format)
            source = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            no_wait = bits.bit(0) == 1
            args = Table.from_io(io, format)
            new channel, reserved1, destination, source, routing_key, no_wait, args, bytesize
          end
        end

        struct BindOk < Exchange
          METHOD_ID = 31_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct Unbind < Exchange
          METHOD_ID = 40_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, destination, source, routing_key, no_wait, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @destination : String,
                         @source : String, @routing_key : String, @no_wait : Bool,
                         @arguments : Table, bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                  # reserved1 (2 bytes)
                         short_string_size(@destination) + # destination ShortString (1 + length)
                         short_string_size(@source) +      # source ShortString (1 + length)
                         short_string_size(@routing_key) + # routing_key ShortString (1 + length)
                         sizeof(Bool) +                    # no_wait boolean (1 byte)
                         @arguments.bytesize.to_u32        # arguments Table
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@destination), format
              io.write_bytes ShortString.new(@source), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_byte @no_wait ? 1_u8 : 0_u8
              io.write_bytes @arguments, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            destination = ShortString.from_io(io, format)
            source = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            no_wait = bits.bit(0) == 1
            args = Table.from_io(io, format)
            new channel, reserved1, destination, source, routing_key, no_wait, args, bytesize
          end
        end

        struct UnbindOk < Exchange
          METHOD_ID = 51_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel, bytesize)
          end
        end
      end

      abstract struct Queue < Method
        CLASS_ID = 50_u16

        def class_id : UInt16
          CLASS_ID
        end

        struct Declare < Queue
          METHOD_ID = 10_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, passive, durable, exclusive, auto_delete, no_wait, arguments, queue_name

          def queue_name=(name)
            @bytesize += name.bytesize - @queue_name.bytesize
            @queue_name = name
          end

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @passive : Bool, @durable : Bool, @exclusive : Bool,
                         @auto_delete : Bool, @no_wait : Bool, @arguments : Table,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                 # reserved1
                         short_string_size(@queue_name) + # queue_name ShortString
                         sizeof(Bool) +                   # bit field
                         @arguments.bytesize.to_u32       # arguments Table
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue_name), format
              bits = 0_u8
              bits = bits | (1 << 0) if @passive
              bits = bits | (1 << 1) if @durable
              bits = bits | (1 << 2) if @exclusive
              bits = bits | (1 << 3) if @auto_delete
              bits = bits | (1 << 4) if @no_wait
              io.write_byte(bits)
              io.write_bytes @arguments, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            name = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            passive = bits.bit(0) == 1
            durable = bits.bit(1) == 1
            exclusive = bits.bit(2) == 1
            auto_delete = bits.bit(3) == 1
            no_wait = bits.bit(4) == 1
            args = Table.from_io(io, format)
            new channel, reserved1, name, passive, durable, exclusive, auto_delete, no_wait, args, bytesize
          end
        end

        struct DeclareOk < Queue
          METHOD_ID = 11_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter queue_name, message_count, consumer_count

          def initialize(channel : UInt16, @queue_name : String, @message_count : UInt32,
                         @consumer_count : UInt32, bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@queue_name) + # queue_name ShortString
                         sizeof(UInt32) +                 # message_count
                         sizeof(UInt32)                   # consumer_count
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@queue_name), format
              io.write_bytes @message_count, format
              io.write_bytes @consumer_count, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            queue_name = ShortString.from_io(io, format)
            message_count = UInt32.from_io(io, format)
            consumer_count = UInt32.from_io(io, format)
            new channel, queue_name, message_count, consumer_count, bytesize
          end
        end

        struct Bind < Queue
          METHOD_ID = 20_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, exchange_name, no_wait, arguments, queue_name, routing_key

          def queue_name=(name)
            @bytesize += name.bytesize - @queue_name.bytesize
            @queue_name = name
          end

          def routing_key=(key)
            @bytesize += key.bytesize - @routing_key.bytesize
            @routing_key = key
          end

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @exchange_name : String, @routing_key : String, @no_wait : Bool,
                         @arguments : Table, bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                    # reserved1 (2 bytes)
                         short_string_size(@queue_name) +    # queue_name ShortString (1 + length)
                         short_string_size(@exchange_name) + # exchange_name ShortString (1 + length)
                         short_string_size(@routing_key) +   # routing_key ShortString (1 + length)
                         sizeof(Bool) +                      # no_wait boolean (1 byte)
                         @arguments.bytesize.to_u32          # arguments Table
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue_name), format
              io.write_bytes ShortString.new(@exchange_name), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_byte @no_wait ? 1_u8 : 0_u8
              io.write_bytes @arguments, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            queue_name = ShortString.from_io(io, format)
            exchange_name = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            no_wait = bits.bit(0) == 1
            args = Table.from_io(io, format)
            new channel, reserved1, queue_name, exchange_name, routing_key, no_wait, args, bytesize
          end
        end

        struct BindOk < Queue
          METHOD_ID = 21_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel, bytesize)
          end
        end

        struct Delete < Queue
          METHOD_ID = 40_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, if_unused, if_empty, no_wait, queue_name

          def queue_name=(name)
            @bytesize += name.bytesize - @queue_name.bytesize
            @queue_name = name
          end

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @if_unused : Bool, @if_empty : Bool, @no_wait : Bool,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                 # reserved1
                         short_string_size(@queue_name) + # queue_name ShortString
                         sizeof(Bool)                     # bit field
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue_name), format
              bits = 0_u8
              bits = bits | (1 << 0) if @if_unused
              bits = bits | (1 << 1) if @if_empty
              bits = bits | (1 << 2) if @no_wait
              io.write_byte(bits)
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            name = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            if_unused = bits.bit(0) == 1
            if_empty = bits.bit(1) == 1
            no_wait = bits.bit(2) == 1
            new channel, reserved1, name, if_unused, if_empty, no_wait, bytesize
          end
        end

        struct DeleteOk < Queue
          METHOD_ID = 41_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter message_count

          def initialize(channel : UInt16, @message_count : UInt32)
            super(channel, 4_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @message_count, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            message_count = UInt32.from_io(io, format)
            new channel, message_count
          end
        end

        struct Unbind < Queue
          METHOD_ID = 50_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, exchange_name, routing_key, arguments, queue_name

          def queue_name=(name)
            @bytesize += name.bytesize - @queue_name.bytesize
            @queue_name = name
          end

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @exchange_name : String, @routing_key : String,
                         @arguments : Table, bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                    # reserved1
                         short_string_size(@queue_name) +    # queue_name ShortString
                         short_string_size(@exchange_name) + # exchange_name ShortString
                         short_string_size(@routing_key) +   # routing_key ShortString
                         @arguments.bytesize.to_u32          # arguments Table
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue_name), format
              io.write_bytes ShortString.new(@exchange_name), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_bytes @arguments, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            queue_name = ShortString.from_io(io, format)
            exchange_name = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            args = Table.from_io(io, format)
            new channel, reserved1, queue_name, exchange_name, routing_key, args, bytesize
          end
        end

        struct UnbindOk < Queue
          METHOD_ID = 51_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct Purge < Queue
          METHOD_ID = 30_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reserved1, no_wait, queue_name

          def queue_name=(name)
            @bytesize += name.bytesize - @queue_name.bytesize
            @queue_name = name
          end

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @no_wait : Bool, bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                 # reserved1
                         short_string_size(@queue_name) + # queue_name ShortString
                         sizeof(Bool)                     # no_wait boolean
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue_name), format
              io.write_byte @no_wait ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            queue_name = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            no_wait = bits.bit(0) == 1
            new channel, reserved1, queue_name, no_wait, bytesize
          end
        end

        struct PurgeOk < Queue
          METHOD_ID = 31_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter message_count

          def initialize(channel : UInt16, @message_count : UInt32)
            super(channel, 4_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @message_count, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            message_count = UInt32.from_io(io, format)
            new channel, message_count
          end
        end
      end

      abstract struct Basic < Method
        CLASS_ID = 60_u16

        def class_id : UInt16
          CLASS_ID
        end

        struct Publish < Basic
          METHOD_ID = 40_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter exchange, routing_key, mandatory, immediate

          def initialize(channel, @reserved1 : UInt16, @exchange : String,
                         @routing_key : String, @mandatory : Bool, @immediate : Bool,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                  # reserved1
                         short_string_size(@exchange) +    # exchange ShortString
                         short_string_size(@routing_key) + # routing_key ShortString
                         sizeof(Bool)                      # bit field
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@exchange), format
              io.write_bytes ShortString.new(@routing_key), format
              bits = 0_u8
              bits = bits | (1 << 0) if @mandatory
              bits = bits | (1 << 1) if @immediate
              io.write_byte(bits)
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            exchange = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            mandatory = bits.bit(0) == 1
            immediate = bits.bit(1) == 1
            new channel, reserved1, exchange, routing_key, mandatory, immediate, bytesize
          end
        end

        struct Deliver < Basic
          METHOD_ID = 60_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter consumer_tag, delivery_tag, redelivered, exchange, routing_key

          def initialize(channel, @consumer_tag : String, @delivery_tag : UInt64,
                         @redelivered : Bool, @exchange : String, @routing_key : String,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@consumer_tag) + # consumer_tag ShortString
                         sizeof(UInt64) +                   # delivery_tag
                         sizeof(Bool) +                     # redelivered boolean
                         short_string_size(@exchange) +     # exchange ShortString
                         short_string_size(@routing_key)    # routing_key ShortString
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@consumer_tag), format
              io.write_bytes @delivery_tag, format
              io.write_byte @redelivered ? 1_u8 : 0_u8
              io.write_bytes ShortString.new(@exchange), format
              io.write_bytes ShortString.new(@routing_key), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            consumer_tag = ShortString.from_io(io, format)
            delivery_tag = UInt64.from_io(io, format)
            redelivered = (io.read_byte || raise IO::EOFError.new) > 0
            exchange = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            new channel, consumer_tag, delivery_tag, redelivered, exchange, routing_key, bytesize
          end
        end

        struct Get < Basic
          METHOD_ID = 70_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter no_ack, queue

          def queue=(name)
            @bytesize += name.bytesize - @queue.bytesize
            @queue = name
          end

          def initialize(channel, @reserved1 : UInt16, @queue : String, @no_ack : Bool,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +            # reserved1
                         short_string_size(@queue) + # queue ShortString
                         sizeof(Bool)                # no_ack boolean
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue), format
              io.write_byte @no_ack ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            queue = ShortString.from_io(io, format)
            no_ack = (io.read_byte || raise IO::EOFError.new) > 0
            new channel, reserved1, queue, no_ack, bytesize
          end
        end

        struct GetOk < Basic
          METHOD_ID = 71_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter delivery_tag, redelivered, exchange, routing_key, message_count

          def initialize(channel, @delivery_tag : UInt64, @redelivered : Bool,
                         @exchange : String, @routing_key : String, @message_count : UInt32,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt64) +                  # delivery_tag
                         sizeof(Bool) +                    # redelivered boolean
                         short_string_size(@exchange) +    # exchange ShortString
                         short_string_size(@routing_key) + # routing_key ShortString
                         sizeof(UInt32)                    # message_count
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @delivery_tag, format
              io.write_byte @redelivered ? 1_u8 : 0_u8
              io.write_bytes ShortString.new(@exchange), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_bytes @message_count, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            delivery_tag = UInt64.from_io(io, format)
            redelivered = (io.read_byte || raise IO::EOFError.new) == 1_u8
            exchange = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            message_count = UInt32.from_io(io, format)
            new channel, delivery_tag, redelivered, exchange, routing_key, message_count, bytesize
          end
        end

        struct GetEmpty < Basic
          METHOD_ID = 72_u16

          def method_id : UInt16
            METHOD_ID
          end

          def initialize(channel, @reserved1 = "", bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@reserved1) # reserved1 ShortString
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@reserved1), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = ShortString.from_io(io, format)
            new channel, reserved1, bytesize
          end
        end

        struct Ack < Basic
          METHOD_ID = 80_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter delivery_tag, multiple

          def initialize(channel, @delivery_tag : UInt64, @multiple : Bool)
            super(channel, 9_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              buf = uninitialized UInt8[9]
              slice = buf.to_slice
              format.encode(@delivery_tag, slice)
              buf[8] = @multiple ? 1u8 : 0u8
              io.write slice
            end
          end

          def self.from_io(channel, bytesize, io, format)
            buf = uninitialized UInt8[9]
            slice = buf.to_slice
            io.read_fully(slice)
            delivery_tag = format.decode(UInt64, slice)
            multiple = slice[8] == 1u8
            new channel, delivery_tag, multiple
          end
        end

        struct Reject < Basic
          METHOD_ID = 90_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter delivery_tag, requeue

          def initialize(channel, @delivery_tag : UInt64, @requeue : Bool)
            super(channel, 9_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              buf = uninitialized UInt8[9]
              slice = buf.to_slice
              format.encode(@delivery_tag, slice)
              buf[8] = @requeue ? 1u8 : 0u8
              io.write slice
            end
          end

          def self.from_io(channel, bytesize, io, format)
            buf = uninitialized UInt8[9]
            slice = buf.to_slice
            io.read_fully(slice)
            delivery_tag = format.decode(UInt64, slice)
            requeue = slice[8] == 1u8
            new channel, delivery_tag, requeue
          end
        end

        struct Nack < Basic
          METHOD_ID = 120_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter delivery_tag, multiple, requeue

          def initialize(channel, @delivery_tag : UInt64, @multiple : Bool, @requeue : Bool)
            super(channel, 9_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              buf = uninitialized UInt8[9]
              slice = buf.to_slice
              format.encode(@delivery_tag, slice)
              buf[8] = @multiple && @requeue ? 3u8 : @requeue ? 2u8 : @multiple ? 1u8 : 0u8
              io.write slice
            end
          end

          def self.from_io(channel, bytesize, io, format)
            buf = uninitialized UInt8[9]
            slice = buf.to_slice
            io.read_fully(slice)
            delivery_tag = format.decode(UInt64, slice)
            multiple = slice[8].bit(0) == 1
            requeue = slice[8].bit(1) == 1
            new channel, delivery_tag, multiple, requeue
          end
        end

        struct Qos < Basic
          METHOD_ID = 10_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter prefetch_size, prefetch_count, global

          def initialize(channel, @prefetch_size : UInt32, @prefetch_count : UInt16, @global : Bool)
            super(channel, 7_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @prefetch_size, format
              io.write_bytes @prefetch_count, format
              io.write_byte @global ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            prefetch_size = UInt32.from_io(io, format)
            prefetch_count = UInt16.from_io(io, format)
            global = (io.read_byte || raise IO::EOFError.new) > 0
            new channel, prefetch_size, prefetch_count, global
          end
        end

        struct QosOk < Basic
          METHOD_ID = 11_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct Consume < Basic
          METHOD_ID = 20_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter no_local, no_ack, exclusive, no_wait, arguments, queue, consumer_tag

          def queue=(name)
            @bytesize += name.bytesize - @queue.bytesize
            @queue = name
          end

          def consumer_tag=(tag)
            @bytesize += tag.bytesize - @consumer_tag.bytesize
            @consumer_tag = tag
          end

          def initialize(channel, @reserved1 : UInt16, @queue : String, @consumer_tag : String,
                         @no_local : Bool, @no_ack : Bool, @exclusive : Bool, @no_wait : Bool,
                         @arguments : Table, bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                   # reserved1
                         short_string_size(@queue) +        # queue ShortString
                         short_string_size(@consumer_tag) + # consumer_tag ShortString
                         sizeof(Bool) +                     # bit field
                         @arguments.bytesize.to_u32         # arguments Table
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue), format
              io.write_bytes ShortString.new(@consumer_tag), format
              bits = 0_u8
              bits = bits | (1 << 0) if @no_local
              bits = bits | (1 << 1) if @no_ack
              bits = bits | (1 << 2) if @exclusive
              bits = bits | (1 << 3) if @no_wait
              io.write_byte(bits)
              io.write_bytes @arguments, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            queue = ShortString.from_io(io, format)
            consumer_tag = ShortString.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            no_local = bits.bit(0) == 1
            no_ack = bits.bit(1) == 1
            exclusive = bits.bit(2) == 1
            no_wait = bits.bit(3) == 1
            args = Table.from_io(io, format)
            new channel, reserved1, queue, consumer_tag, no_local, no_ack, exclusive, no_wait, args, bytesize
          end
        end

        struct ConsumeOk < Basic
          METHOD_ID = 21_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter consumer_tag

          def initialize(channel, @consumer_tag : String, bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@consumer_tag) # consumer_tag ShortString
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@consumer_tag), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            tag = ShortString.from_io(io, format)
            new(channel, tag, bytesize)
          end
        end

        struct Return < Basic
          METHOD_ID = 50_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter reply_code, reply_text, exchange, routing_key

          def initialize(channel, @reply_code : UInt16, @reply_text : String,
                         @exchange : String, @routing_key : String,
                         bytesize = nil)
            if bytesize.nil?
              bytesize = sizeof(UInt16) +                 # reply_code
                         short_string_size(@reply_text) + # reply_text ShortString
                         short_string_size(@exchange) +   # exchange ShortString
                         short_string_size(@routing_key)  # routing_key ShortString
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@reply_code, format)
              io.write_bytes ShortString.new(@reply_text), format
              io.write_bytes ShortString.new(@exchange), format
              io.write_bytes ShortString.new(@routing_key), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reply_code = UInt16.from_io(io, format)
            reply_text = ShortString.from_io(io, format)
            exchange = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            new(channel, reply_code, reply_text, exchange, routing_key, bytesize)
          end
        end

        struct Cancel < Basic
          METHOD_ID = 30_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter consumer_tag, no_wait

          def initialize(channel : UInt16, @consumer_tag : String, @no_wait : Bool, bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@consumer_tag) + # consumer_tag ShortString
                         sizeof(Bool)                       # no_wait boolean
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@consumer_tag), format
              io.write_byte @no_wait ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            consumer_tag = ShortString.from_io(io, format)
            no_wait = (io.read_byte || raise IO::EOFError.new) > 0
            new(channel, consumer_tag, no_wait, bytesize)
          end
        end

        struct CancelOk < Basic
          METHOD_ID = 31_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter consumer_tag

          def initialize(channel : UInt16, @consumer_tag : String, bytesize = nil)
            if bytesize.nil?
              bytesize = short_string_size(@consumer_tag) # consumer_tag ShortString
            end
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@consumer_tag), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            consumer_tag = ShortString.from_io(io, format)
            new(channel, consumer_tag, bytesize)
          end
        end

        struct Recover < Basic
          METHOD_ID = 110_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter requeue

          def initialize(channel : UInt16, @requeue : Bool)
            super(channel, 1_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_byte @requeue ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            requeue = (io.read_byte || raise IO::EOFError.new) > 0
            new(channel, requeue)
          end
        end

        struct RecoverOk < Basic
          METHOD_ID = 111_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end
      end

      abstract struct Confirm < Method
        CLASS_ID = 85_u16

        def class_id : UInt16
          CLASS_ID
        end

        struct Select < Confirm
          METHOD_ID = 10_u16

          def method_id : UInt16
            METHOD_ID
          end

          getter no_wait

          def initialize(channel : UInt16, @no_wait : Bool)
            super(channel, 1_u32)
          end

          def self.from_io(channel, bytesize, io, format)
            new channel, (io.read_byte || raise IO::EOFError.new) > 0
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_byte @no_wait ? 1_u8 : 0_u8
            end
          end
        end

        struct SelectOk < Confirm
          METHOD_ID = 11_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end
      end

      abstract struct Tx < Method
        CLASS_ID = 90_u16

        def class_id : UInt16
          CLASS_ID
        end

        struct Select < Tx
          METHOD_ID = 10_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct SelectOk < Tx
          METHOD_ID = 11_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct Commit < Tx
          METHOD_ID = 20_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct CommitOk < Tx
          METHOD_ID = 21_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct Rollback < Tx
          METHOD_ID = 30_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end

        struct RollbackOk < Tx
          METHOD_ID = 31_u16

          def method_id : UInt16
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            new(channel)
          end
        end
      end
    end
  end
end
