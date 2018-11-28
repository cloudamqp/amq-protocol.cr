module AMQ
  module Protocol
    abstract struct Frame
      getter channel, bytesize

      def initialize(@channel : UInt16, @bytesize : UInt32)
      end

      abstract def to_io(io, format)
      abstract def type : UInt8

      def wrap(io, format : IO::ByteFormat)
        io.write_byte type
        io.write_bytes @channel, format
        io.write_bytes @bytesize, format
        yield
        io.write_byte 206_u8
      end

      def self.from_io(io, format = IO::ByteFormat::NetworkEndian, &block : Frame -> _)
        type = io.read_byte || raise(IO::EOFError.new)
        channel = UInt16.from_io(io, format)
        size = UInt32.from_io(io, format)
        frame =
          case type
          when Method::TYPE    then Method.from_io(channel, size, io, format)
          when Header::TYPE    then Header.from_io(channel, size, io, format)
          when Body::TYPE      then Body.new(channel, size, io)
          when Heartbeat::TYPE then Heartbeat.new
          else
            raise Error::NotImplemented.new channel, 0_u16, 0_u16
          end
        yield frame
      rescue ex : IO::Error | Errno
        raise ex
      rescue ex
        raise Error::FrameDecode.new(ex.message, ex)
      ensure
        begin
          if !io.closed?
            if (frame_end = io.read_byte) && frame_end != 206_u8
              raise Error::InvalidFrameEnd.new("#{frame.class}-end was #{frame_end.to_s}, expected 206")
            end
          end
        rescue ex : IO::Error | Errno
        end
      end

      struct Header < Frame
        TYPE = 2_u8

        def type
          TYPE
        end

        getter body_size, properties

        def initialize(channel : UInt16, @class_id : UInt16, @weight : UInt16, @body_size : UInt64,
                       @properties : Properties, bytesize = nil)
          bytesize ||= sizeof(UInt16) + sizeof(UInt16) + sizeof(UInt64) + @properties.bytesize
          super(channel, bytesize.to_u32)
        end

        def to_io(io : IO, format : IO::ByteFormat)
          wrap(io, format) do
            io.write_bytes @class_id, format
            io.write_bytes @weight, format
            io.write_bytes @body_size, format
            io.write_bytes @properties, format
          end
        end

        def self.from_io(channel, bytesize, io, format)
          class_id = UInt16.from_io(io, format)
          weight = UInt16.from_io(io, format)
          body_size = UInt64.from_io(io, format)
          props = Properties.from_io(io, format)
          self.new channel, class_id, weight, body_size, props, bytesize
        end
      end

      struct Body < Frame
        TYPE = 3_u8

        def type
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
              raise Error::FrameEncode.new("Could not write the full body")
            end
          end
        end
      end

      struct Heartbeat < Frame
        TYPE = 8_u8

        def type
          TYPE
        end

        def initialize
          @channel = 0_u16
          @bytesize = 0_u32
        end

        def to_io(io, format)
          wrap(io, format) { }
        end
      end

      alias MessageFrame = Body | Header | Method::Basic::Publish

      abstract struct Method < Frame
        TYPE = 1_u8

        def type
          TYPE
        end

        def initialize(channel : UInt16, bytesize = 0_u32)
          super(channel, bytesize + 2 * sizeof(UInt16))
        end

        abstract def class_id : UInt16
        abstract def method_id : UInt16

        def wrap(io, format)
          super(io, format) do
            io.write_bytes class_id, format
            io.write_bytes method_id, format
            yield
          end
        end

        def self.from_io(channel, bytesize, io, format)
          class_id = UInt16.from_io(io, format)
          bytesize -= sizeof(UInt16)
          case class_id
          when 10_u16 then Connection.from_io(channel, bytesize, io, format)
          when 20_u16 then Channel.from_io(channel, bytesize, io, format)
          when 40_u16 then Exchange.from_io(channel, bytesize, io, format)
          when 50_u16 then Queue.from_io(channel, bytesize, io, format)
          when 60_u16 then Basic.from_io(channel, bytesize, io, format)
          when 85_u16 then Confirm.from_io(channel, bytesize, io, format)
          when 90_u16 then Tx.from_io(channel, bytesize, io, format)
          else
            raise Error::NotImplemented.new(channel, class_id, 0_u16)
          end
        end
      end

      abstract struct Connection < Method
        CLASS_ID = 10_u16

        def class_id
          CLASS_ID
        end

        def initialize(bytesize = 0_u32)
          super(0_u16, bytesize)
        end

        def self.from_io(channel, bytesize, io, format)
          method_id = UInt16.from_io(io, format)
          bytesize -= sizeof(UInt16)
          case method_id
          when 10_u16 then Start.from_io(io, bytesize, format)
          when 11_u16 then StartOk.from_io(io, bytesize, format)
          when 30_u16 then Tune.from_io(io, bytesize, format)
          when 31_u16 then TuneOk.from_io(io, bytesize, format)
          when 40_u16 then Open.from_io(io, bytesize, format)
          when 41_u16 then OpenOk.from_io(io, bytesize, format)
          when 50_u16 then Close.from_io(io, bytesize, format)
          when 51_u16 then CloseOk.from_io(io, bytesize, format)
          else             raise Error::NotImplemented.new(channel, CLASS_ID, method_id)
          end
        end

        struct Start < Connection
          METHOD_ID = 10_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_byte(@version_major)
              io.write_byte(@version_minor)
              io.write_bytes Table.new(@server_properties), format
              io.write_bytes LongString.new(@mechanisms), format
              io.write_bytes LongString.new(@locales), format
            end
          end

          getter server_properties

          def initialize(@version_major = 0_u8, @version_minor = 9_u8,
                         @server_properties : Hash(String, Field) = {
                           "capabilities" => {
                             "publisher_confirms"           => true,
                             "exchange_exchange_bindings"   => true,
                             "basic.nack"                   => true,
                             "per_consumer_qos"             => true,
                             "authentication_failure_close" => true,
                             "consumer_cancel_notify"       => true,
                           } of String => Field,
                         } of String => Field,
                         @mechanisms = "PLAIN", @locales = "en_US",
                         bytesize = nil)
            bytesize ||= 1 + 1 + Table.new(@server_properties).bytesize + 4 +
                         @mechanisms.bytesize + 4 + @locales.bytesize
            super(bytesize.to_u32)
          end

          def self.from_io(io, bytesize, format)
            version_major = io.read_byte || raise IO::EOFError.new
            version_minor = io.read_byte || raise IO::EOFError.new
            server_properties = Table.from_io(io, format)
            mech = LongString.from_io(io, format)
            locales = LongString.from_io(io, format)
            self.new(version_major, version_minor, server_properties, mech, locales, bytesize)
          end
        end

        struct StartOk < Connection
          getter client_properties, mechanism, response, locale

          METHOD_ID = 11_u16

          def method_id
            METHOD_ID
          end

          def initialize(@client_properties : Hash(String, Field), @mechanism : String,
                         @response : String, @locale : String, bytesize = nil)
            bytesize ||= Table.new(@client_properties).bytesize + 1 + @mechanism.bytesize + 4 +
                         @response.bytesize + 1 + @locale.bytesize
            super(bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes Table.new(@client_properties), format
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
            self.new(props, mech, auth, locale, bytesize)
          end
        end

        struct Tune < Connection
          getter channel_max, frame_max, heartbeat
          METHOD_ID = 30_u16

          def method_id
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
            self.new(channel_max, frame_max, heartbeat)
          end
        end

        struct TuneOk < Connection
          getter channel_max, frame_max, heartbeat
          METHOD_ID = 31_u16

          def method_id
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
            self.new(channel_max, frame_max, heartbeat)
          end
        end

        struct Open < Connection
          getter vhost, reserved1, reserved2
          METHOD_ID = 40_u16

          def method_id
            METHOD_ID
          end

          def initialize(@vhost = "/", @reserved1 = "", @reserved2 = false, bytesize = nil)
            bytesize ||= 1 + @vhost.bytesize + 1 + @reserved1.bytesize + 1
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
            self.new(vhost, reserved1, reserved2, bytesize)
          end
        end

        struct OpenOk < Connection
          getter reserved1

          METHOD_ID = 41_u16

          def method_id
            METHOD_ID
          end

          def initialize(@reserved1 = "", bytesize = nil)
            bytesize ||= 1 + @reserved1.bytesize
            super(bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@reserved1), format
            end
          end

          def self.from_io(io, bytesize, format)
            reserved1 = ShortString.from_io(io, format)
            self.new(reserved1, bytesize)
          end
        end

        struct Close < Connection
          getter reply_code, reply_text, failing_class_id, failing_method_id

          METHOD_ID = 50_u16

          def method_id
            METHOD_ID
          end

          def initialize(@reply_code : UInt16, @reply_text : String, @failing_class_id : UInt16,
                         @failing_method_id : UInt16, bytesize = nil)
            bytesize ||= 2 + 1 + @reply_text.bytesize + 2 + 2
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
            self.new(code, text, failing_class_id, failing_method_id, bytesize)
          end
        end

        struct CloseOk < Connection
          METHOD_ID = 51_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(io, bytesize, format)
            self.new
          end
        end
      end

      abstract struct Channel < Method
        CLASS_ID = 20_u16

        def class_id
          CLASS_ID
        end

        def self.from_io(channel, bytesize, io, format)
          method_id = UInt16.from_io(io, format)
          bytesize -= sizeof(UInt16)
          case method_id
          when 10_u16 then Open.from_io(channel, bytesize, io, format)
          when 11_u16 then OpenOk.from_io(channel, bytesize, io, format)
            # when 20_u16 then Flow.from_io(channel, bytesize, io, format)
            # when 21_u16 then FlowOk.from_io(channel, bytesize, io, format)
          when 40_u16 then Close.from_io(channel, bytesize, io, format)
          when 41_u16 then CloseOk.from_io(channel, bytesize, io, format)
          else             raise Error::NotImplemented.new(channel, CLASS_ID, method_id)
          end
        end

        struct Open < Channel
          METHOD_ID = 10_u16

          def method_id
            METHOD_ID
          end

          getter reserved1

          def initialize(channel : UInt16, @reserved1 = "", bytesize = nil)
            bytesize ||= 1 + @reserved1.bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@reserved1), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = ShortString.from_io(io, format)
            Open.new channel, reserved1, bytesize
          end
        end

        struct OpenOk < Channel
          METHOD_ID = 11_u16

          def method_id
            METHOD_ID
          end

          getter reserved1

          def initialize(channel : UInt16, @reserved1 = "", bytesize = nil)
            bytesize ||= 4 + @reserved1.bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes LongString.new(@reserved1), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = LongString.from_io(io, format)
            OpenOk.new channel, reserved1, bytesize
          end
        end

        struct Close < Channel
          METHOD_ID = 40_u16

          def method_id
            METHOD_ID
          end

          getter reply_code, reply_text, classid, methodid

          def initialize(channel : UInt16, @reply_code : UInt16, @reply_text : String,
                         @classid : UInt16, @methodid : UInt16, bytesize = nil)
            bytesize ||= 2 + 1 + @reply_text.bytesize + 2 + 2
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
            self.new channel, reply_code, reply_text, classid, methodid, bytesize
          end
        end

        struct CloseOk < Channel
          METHOD_ID = 41_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new channel
          end
        end
      end

      abstract struct Exchange < Method
        CLASS_ID = 40_u16

        def class_id
          CLASS_ID
        end

        def self.from_io(channel, bytesize, io, format)
          method_id = UInt16.from_io(io, format)
          bytesize -= sizeof(UInt16)
          case method_id
          when 10_u16 then Declare.from_io(channel, bytesize, io, format)
          when 11_u16 then DeclareOk.from_io(channel, bytesize, io, format)
          when 20_u16 then Delete.from_io(channel, bytesize, io, format)
          when 21_u16 then DeleteOk.from_io(channel, bytesize, io, format)
          when 30_u16 then Bind.from_io(channel, bytesize, io, format)
          when 31_u16 then BindOk.from_io(channel, bytesize, io, format)
          when 40_u16 then Unbind.from_io(channel, bytesize, io, format)
          when 51_u16 then UnbindOk.from_io(channel, bytesize, io, format)
          else             raise Error::NotImplemented.new(channel, CLASS_ID, method_id)
          end
        end

        struct Declare < Exchange
          METHOD_ID = 10_u16

          def method_id
            METHOD_ID
          end

          getter reserved1, exchange_name, exchange_type, passive, durable, auto_delete, internal, no_wait, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @exchange_name : String,
                         @exchange_type : String, @passive : Bool, @durable : Bool, @auto_delete : Bool,
                         @internal : Bool, @no_wait : Bool, @arguments : Hash(String, Field),
                         bytesize = nil)
            bytesize ||= 2 + 2 + 1 + @exchange_name.bytesize + 1 + @exchange_type.bytesize + 1 + Table.new(@arguments).bytesize
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
              io.write_bytes Table.new(@arguments), format
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

            self.new channel, reserved1, name, type, passive, durable, auto_delete, internal, no_wait, args, bytesize
          end
        end

        struct DeclareOk < Exchange
          METHOD_ID = 11_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel)
          end
        end

        struct Delete < Exchange
          METHOD_ID = 20_u16

          def method_id
            METHOD_ID
          end

          getter reserved1, exchange_name, if_unused, no_wait

          def initialize(channel : UInt16, @reserved1 : UInt16, @exchange_name : String,
                         @if_unused : Bool, @no_wait : Bool, bytesize = nil)
            bytesize ||= 2 + 1 + @exchange_name.bytesize + 1
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
            self.new channel, reserved1, name, if_unused, no_wait, bytesize
          end
        end

        struct DeleteOk < Exchange
          METHOD_ID = 21_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel)
          end
        end

        struct Bind < Exchange
          METHOD_ID = 30_u16

          def method_id
            METHOD_ID
          end

          getter reserved1, destination, source, routing_key, no_wait, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @destination : String,
                         @source : String, @routing_key : String, @no_wait : Bool,
                         @arguments : Hash(String, Field),
                         bytesize = nil)
            bytesize ||= 2 + 2 + 1 + @destination.bytesize + 1 + @source.bytesize + 1 +
                         @routing_key.bytesize + 1 + Table.new(@arguments).bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@destination), format
              io.write_bytes ShortString.new(@source), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_byte @no_wait ? 1_u8 : 0_u8
              io.write_bytes Table.new(@arguments), format
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
            self.new channel, reserved1, destination, source, routing_key, no_wait, args, bytesize
          end
        end

        struct BindOk < Exchange
          METHOD_ID = 31_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel)
          end
        end

        struct Unbind < Exchange
          METHOD_ID = 40_u16

          def method_id
            METHOD_ID
          end

          getter reserved1, destination, source, routing_key, no_wait, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @destination : String,
                         @source : String, @routing_key : String, @no_wait : Bool,
                         @arguments : Hash(String, Field), bytesize = nil)
            bytesize ||= 2 + 1 + @destination.bytesize + 1 + @source.bytesize + 1 +
                         @routing_key.bytesize + 1 + Table.new(@arguments).bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@destination), format
              io.write_bytes ShortString.new(@source), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_byte @no_wait ? 1_u8 : 0_u8
              io.write_bytes Table.new(@arguments), format
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
            self.new channel, reserved1, destination, source, routing_key, no_wait, args, bytesize
          end
        end

        struct UnbindOk < Exchange
          METHOD_ID = 51_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel, bytesize)
          end
        end
      end

      abstract struct Queue < Method
        CLASS_ID = 50_u16

        def class_id
          CLASS_ID
        end

        def self.from_io(channel, bytesize, io, format)
          method_id = UInt16.from_io(io, format)
          bytesize -= sizeof(UInt16)
          case method_id
          when 10_u16 then Declare.from_io(channel, bytesize, io, format)
          when 11_u16 then DeclareOk.from_io(channel, bytesize, io, format)
          when 20_u16 then Bind.from_io(channel, bytesize, io, format)
          when 21_u16 then BindOk.from_io(channel, bytesize, io, format)
          when 30_u16 then Purge.from_io(channel, bytesize, io, format)
          when 31_u16 then PurgeOk.from_io(channel, bytesize, io, format)
          when 40_u16 then Delete.from_io(channel, bytesize, io, format)
          when 41_u16 then DeleteOk.from_io(channel, bytesize, io, format)
          when 50_u16 then Unbind.from_io(channel, bytesize, io, format)
          when 51_u16 then UnbindOk.from_io(channel, bytesize, io, format)
          else             raise Error::NotImplemented.new(channel, CLASS_ID, method_id)
          end
        end

        struct Declare < Queue
          METHOD_ID = 10_u16

          def method_id
            METHOD_ID
          end

          property reserved1, queue_name, passive, durable, exclusive, auto_delete, no_wait, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @passive : Bool, @durable : Bool, @exclusive : Bool,
                         @auto_delete : Bool, @no_wait : Bool, @arguments : Hash(String, Field),
                         bytesize = nil)
            bytesize ||= 2 + 1 + @queue_name.bytesize + 1 + Table.new(@arguments).bytesize
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
              io.write_bytes Table.new(@arguments), format
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
            self.new channel, reserved1, name, passive, durable, exclusive, auto_delete, no_wait, args, bytesize
          end
        end

        struct DeclareOk < Queue
          METHOD_ID = 11_u16

          def method_id
            METHOD_ID
          end

          getter queue_name, message_count, consumer_count

          def initialize(channel : UInt16, @queue_name : String, @message_count : UInt32,
                         @consumer_count : UInt32, bytesize = nil)
            bytesize ||= 1 + @queue_name.bytesize + 4 + 4
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
            self.new channel, queue_name, message_count, consumer_count, bytesize
          end
        end

        struct Bind < Queue
          METHOD_ID = 20_u16

          def method_id
            METHOD_ID
          end

          getter reserved1, queue_name, exchange_name, routing_key, no_wait, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @exchange_name : String, @routing_key : String, @no_wait : Bool,
                         @arguments : Hash(String, Field), bytesize = nil)
            bytesize ||= 2 + 1 + @queue_name.bytesize + 1 + @exchange_name.bytesize + 1 +
                         @routing_key.bytesize + 1 + Table.new(@arguments).bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue_name), format
              io.write_bytes ShortString.new(@exchange_name), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_byte @no_wait ? 1_u8 : 0_u8
              io.write_bytes Table.new(@arguments), format
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
            self.new channel, reserved1, queue_name, exchange_name, routing_key, no_wait, args, bytesize
          end
        end

        struct BindOk < Queue
          METHOD_ID = 21_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel, bytesize)
          end
        end

        struct Delete < Queue
          METHOD_ID = 40_u16

          def method_id
            METHOD_ID
          end

          getter reserved1, queue_name, if_unused, if_empty, no_wait

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @if_unused : Bool, @if_empty : Bool, @no_wait : Bool,
                         bytesize = nil)
            bytesize ||= 2 + 1 + @queue_name.bytesize + 1
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
            self.new channel, reserved1, name, if_unused, if_empty, no_wait, bytesize
          end
        end

        struct DeleteOk < Queue
          METHOD_ID = 41_u16

          def method_id
            METHOD_ID
          end

          def initialize(channel : UInt16, @message_count : UInt32)
            super(channel, 4_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @message_count, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            raise Error::NotImplemented.new(channel, CLASS_ID, METHOD_ID)
          end
        end

        struct Unbind < Queue
          METHOD_ID = 50_u16

          def method_id
            METHOD_ID
          end

          getter reserved1, queue_name, exchange_name, routing_key, arguments

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @exchange_name : String, @routing_key : String,
                         @arguments : Hash(String, Field), bytesize = nil)
            bytesize ||= 2 + 1 + @queue_name.bytesize + 1 + @exchange_name.bytesize + 1 +
                         @routing_key.bytesize + Table.new(@arguments).bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
              io.write_bytes ShortString.new(@queue_name), format
              io.write_bytes ShortString.new(@exchange_name), format
              io.write_bytes ShortString.new(@routing_key), format
              io.write_bytes Table.new(@arguments), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            queue_name = ShortString.from_io(io, format)
            exchange_name = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            args = Table.from_io(io, format)
            self.new channel, reserved1, queue_name, exchange_name, routing_key, args, bytesize
          end
        end

        struct UnbindOk < Queue
          METHOD_ID = 51_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel)
          end
        end

        struct Purge < Queue
          METHOD_ID = 30_u16

          def method_id
            METHOD_ID
          end

          getter reserved1, queue_name, no_wait

          def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                         @no_wait : Bool, bytesize = nil)
            bytesize ||= 2 + 1 + @queue_name.bytesize + 1
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
            self.new channel, reserved1, queue_name, no_wait, bytesize
          end
        end

        struct PurgeOk < Queue
          METHOD_ID = 31_u16

          def method_id
            METHOD_ID
          end

          def initialize(channel : UInt16, @message_count : UInt32)
            super(channel, 4_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @message_count, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            raise Error::NotImplemented.new(channel, CLASS_ID, METHOD_ID)
          end
        end
      end

      abstract struct Basic < Method
        CLASS_ID = 60_u16

        def class_id
          CLASS_ID
        end

        def self.from_io(channel, bytesize, io, format)
          method_id = UInt16.from_io(io, format)
          bytesize -= sizeof(UInt16)
          case method_id
          when 10_u16 then Qos.from_io(channel, bytesize, io, format)
          when 11_u16 then QosOk.from_io(channel, bytesize, io, format)
          when 20_u16 then Consume.from_io(channel, bytesize, io, format)
          when 21_u16 then ConsumeOk.from_io(channel, bytesize, io, format)
          when 30_u16 then Cancel.from_io(channel, bytesize, io, format)
          when 31_u16 then CancelOk.from_io(channel, bytesize, io, format)
          when 40_u16 then Publish.from_io(channel, bytesize, io, format)
          when 50_u16 then Return.from_io(channel, bytesize, io, format)
          when 60_u16 then Deliver.from_io(channel, bytesize, io, format)
          when 70_u16 then Get.from_io(channel, bytesize, io, format)
          when 71_u16 then GetOk.from_io(channel, bytesize, io, format)
          when 72_u16 then GetEmpty.from_io(channel, bytesize, io, format)
          when 80_u16 then Ack.from_io(channel, bytesize, io, format)
          when 90_u16 then Reject.from_io(channel, bytesize, io, format)
            # when 100_u16 then RecoverAsync.from_io(channel, io, format)
          when 110_u16 then Recover.from_io(channel, bytesize, io, format)
          when 111_u16 then RecoverOk.from_io(channel, bytesize, io, format)
          when 120_u16 then Nack.from_io(channel, bytesize, io, format)
          else              raise Error::NotImplemented.new(channel, CLASS_ID, method_id)
          end
        end

        struct Publish < Basic
          METHOD_ID = 40_u16

          def method_id
            METHOD_ID
          end

          getter exchange, routing_key, mandatory, immediate

          def initialize(channel, @reserved1 : UInt16, @exchange : String,
                         @routing_key : String, @mandatory : Bool, @immediate : Bool,
                         bytesize = nil)
            bytesize ||= 2 + 1 + @exchange.bytesize + 1 + @routing_key.bytesize + 1
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
            self.new channel, reserved1, exchange, routing_key, mandatory, immediate, bytesize
          end
        end

        struct Deliver < Basic
          METHOD_ID = 60_u16

          def method_id
            METHOD_ID
          end

          getter consumer_tag, delivery_tag, redelivered, exchange, routing_key

          def initialize(channel, @consumer_tag : String, @delivery_tag : UInt64,
                         @redelivered : Bool, @exchange : String, @routing_key : String,
                         bytesize = nil)
            bytesize ||= 1 + @consumer_tag.bytesize + sizeof(UInt64) + 1 + 1 +
                         @exchange.bytesize + 1 + @routing_key.bytesize
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
            self.new channel, consumer_tag, delivery_tag, redelivered, exchange, routing_key, bytesize
          end
        end

        struct Get < Basic
          METHOD_ID = 70_u16

          def method_id
            METHOD_ID
          end

          getter queue, no_ack

          def initialize(channel, @reserved1 : UInt16, @queue : String, @no_ack : Bool,
                         bytesize = nil)
            bytesize ||= sizeof(UInt16) + 1 + @queue.bytesize + 1
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
            self.new channel, reserved1, queue, no_ack, bytesize
          end
        end

        struct GetOk < Basic
          METHOD_ID = 71_u16

          def method_id
            METHOD_ID
          end

          getter delivery_tag, redelivered, exchange, routing_key, message_count

          def initialize(channel, @delivery_tag : UInt64, @redelivered : Bool,
                         @exchange : String, @routing_key : String, @message_count : UInt32,
                         bytesize = nil)
            bytesize ||= sizeof(UInt64) + 1 + 1 + @exchange.bytesize + 1 +
                         @routing_key.bytesize + sizeof(UInt32)
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
            self.new channel, delivery_tag, redelivered, exchange, routing_key, message_count, bytesize
          end
        end

        struct GetEmpty < Basic
          METHOD_ID = 72_u16

          def method_id
            METHOD_ID
          end

          def initialize(channel, @reserved1 = 0_u16)
            super(channel, 2_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes @reserved1, format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reserved1 = UInt16.from_io(io, format)
            self.new channel, reserved1
          end
        end

        struct Ack < Basic
          METHOD_ID = 80_u16

          def method_id
            METHOD_ID
          end

          getter :delivery_tag, :multiple

          def initialize(channel, @delivery_tag : UInt64, @multiple : Bool)
            super(channel, 9_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@delivery_tag, format)
              io.write_byte @multiple ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            delivery_tag = UInt64.from_io(io, format)
            multiple = (io.read_byte || raise IO::EOFError.new) > 0
            self.new channel, delivery_tag, multiple
          end
        end

        struct Reject < Basic
          METHOD_ID = 90_u16

          def method_id
            METHOD_ID
          end

          getter :delivery_tag, :requeue

          def initialize(channel, @delivery_tag : UInt64, @requeue : Bool)
            super(channel, 9_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@delivery_tag, format)
              io.write_byte @requeue ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            delivery_tag = UInt64.from_io(io, format)
            requeue = (io.read_byte || raise IO::EOFError.new) > 0
            self.new channel, delivery_tag, requeue
          end
        end

        struct Nack < Basic
          METHOD_ID = 120_u16

          def method_id
            METHOD_ID
          end

          getter :delivery_tag, :multiple, :requeue

          def initialize(channel, @delivery_tag : UInt64, @multiple : Bool, @requeue : Bool)
            super(channel, 10_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@delivery_tag, format)
              io.write_byte @multiple ? 1_u8 : 0_u8
              io.write_byte @requeue ? 1_u8 : 0_u8
            end
          end

          def self.from_io(channel, bytesize, io, format)
            delivery_tag = UInt64.from_io(io, format)
            bits = io.read_byte || raise IO::EOFError.new
            multiple = bits.bit(0) == 1
            requeue = bits.bit(1) == 1
            self.new channel, delivery_tag, multiple, requeue
          end
        end

        struct Qos < Basic
          METHOD_ID = 10_u16

          def method_id
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
            self.new channel, prefetch_size, prefetch_count, global
          end
        end

        struct QosOk < Basic
          METHOD_ID = 11_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel)
          end
        end

        struct Consume < Basic
          METHOD_ID = 20_u16

          def method_id
            METHOD_ID
          end

          property queue, consumer_tag, no_local, no_ack, exclusive, no_wait, arguments

          def initialize(channel, @reserved1 : UInt16, @queue : String, @consumer_tag : String,
                         @no_local : Bool, @no_ack : Bool, @exclusive : Bool, @no_wait : Bool,
                         @arguments : Hash(String, Field), bytesize = nil)
            bytesize ||= 2 + 1 + @queue.bytesize + 1 + @consumer_tag.bytesize + 1 +
                         Table.new(@arguments).bytesize
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
              io.write_bytes Table.new(@arguments), format
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
            self.new channel, reserved1, queue, consumer_tag, no_local, no_ack, exclusive, no_wait, args, bytesize
          end
        end

        struct ConsumeOk < Basic
          METHOD_ID = 21_u16

          def method_id
            METHOD_ID
          end

          getter consumer_tag

          def initialize(channel, @consumer_tag : String, bytesize = nil)
            bytesize ||= 1 + @consumer_tag.bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@consumer_tag), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            tag = ShortString.from_io(io, format)
            self.new(channel, tag, bytesize)
          end
        end

        struct Return < Basic
          METHOD_ID = 50_u16

          def method_id
            METHOD_ID
          end

          getter reply_code, reply_text, exchange_name, routing_key

          def initialize(channel, @reply_code : UInt16, @reply_text : String,
                         @exchange_name : String, @routing_key : String,
                         bytesize = nil)
            bytesize ||= 2 + 1 + @reply_text.bytesize + 1 + @exchange_name.bytesize + 1 +
                         @routing_key.bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes(@reply_code, format)
              io.write_bytes ShortString.new(@reply_text), format
              io.write_bytes ShortString.new(@exchange_name), format
              io.write_bytes ShortString.new(@routing_key), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            reply_code = UInt16.from_io(io, format)
            reply_text = ShortString.from_io(io, format)
            exchange_name = ShortString.from_io(io, format)
            routing_key = ShortString.from_io(io, format)
            self.new(channel, reply_code, reply_text, exchange_name, routing_key, bytesize)
          end
        end

        struct Cancel < Basic
          METHOD_ID = 30_u16

          def method_id
            METHOD_ID
          end

          getter consumer_tag, no_wait

          def initialize(channel : UInt16, @consumer_tag : String, @no_wait : Bool, bytesize = nil)
            bytesize ||= 1 + @consumer_tag.bytesize + 1
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
            self.new(channel, consumer_tag, no_wait, bytesize)
          end
        end

        struct CancelOk < Basic
          METHOD_ID = 31_u16

          def method_id
            METHOD_ID
          end

          getter consumer_tag

          def initialize(channel : UInt16, @consumer_tag : String, bytesize = nil)
            bytesize ||= 1 + @consumer_tag.bytesize
            super(channel, bytesize.to_u32)
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_bytes ShortString.new(@consumer_tag), format
            end
          end

          def self.from_io(channel, bytesize, io, format)
            consumer_tag = ShortString.from_io(io, format)
            self.new(channel, consumer_tag, bytesize)
          end
        end

        struct Recover < Basic
          METHOD_ID = 110_u16

          def method_id
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
            self.new(channel, requeue)
          end
        end

        struct RecoverOk < Basic
          METHOD_ID = 111_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel)
          end
        end
      end

      abstract struct Confirm < Method
        CLASS_ID = 85_u16

        def class_id
          CLASS_ID
        end

        def self.from_io(channel, bytesize, io, format)
          method_id = UInt16.from_io(io, format)
          bytesize -= sizeof(UInt16)
          case method_id
          when 10_u16 then Select.from_io(channel, bytesize, io, format)
          when 11_u16 then SelectOk.from_io(channel, bytesize, io, format)
          else             raise Error::NotImplemented.new(channel, CLASS_ID, method_id)
          end
        end

        struct Select < Confirm
          METHOD_ID = 10_u16

          def method_id
            METHOD_ID
          end

          getter no_wait

          def initialize(channel : UInt16, @no_wait : Bool)
            super(channel, 1_u32)
          end

          def self.from_io(channel, bytesize, io, format)
            self.new channel, (io.read_byte || raise IO::EOFError.new) > 0
          end

          def to_io(io, format)
            wrap(io, format) do
              io.write_byte @no_wait ? 1_u8 : 0_u8
            end
          end
        end

        struct SelectOk < Confirm
          METHOD_ID = 11_u16

          def method_id
            METHOD_ID
          end

          def to_io(io, format)
            wrap(io, format) { }
          end

          def self.from_io(channel, bytesize, io, format)
            self.new(channel)
          end
        end
      end

      abstract struct Tx < Method
        CLASS_ID = 90_u16

        def class_id
          CLASS_ID
        end

        def self.from_io(channel, bytesize, io, format)
          method_id = UInt16.from_io(io, format)
          bytesize -= sizeof(UInt16)
          raise Error::NotImplemented.new(channel, CLASS_ID, method_id)
        end
      end
    end
  end
end
