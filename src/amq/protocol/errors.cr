module AMQ
  module Protocol
    class Error < Exception
      class FrameDecode < Error; end

      class FrameEncode < Error; end

      class InvalidFrameEnd < Error; end

      class NotImplemented < Error
        getter channel, class_id, method_id

        def initialize(@channel : UInt16, @class_id : UInt16, @method_id : UInt16)
          super("Method id #{@method_id} not implemented in class #{@class_id} (Channel #{@channel})")
        end

        def initialize(frame : Method)
          @channel = frame.channel
          @class_id = frame.class_id
          @method_id = frame.method_id
          super("Method id #{@method_id} not implemented in class #{@class_id}")
        end

        def initialize(frame : Frame)
          @channel = 0_u16
          @class_id = 0_u16
          @method_id = 0_u16
          super("Frame type #{frame.type} not implemented")
        end
      end
    end
  end
end
