module AMQ
  module Protocol
    module V1
      abstract struct Frame
        getter bytesize : UInt32
        getter doff : UInt8
        getter type : UInt8
      end

      abstract struct AMQP < Frame
        TYPE = 0_u8

        getter channel : UInt16

        def type : UInt8
          TYPE
        end
      end
    end
  end
end
