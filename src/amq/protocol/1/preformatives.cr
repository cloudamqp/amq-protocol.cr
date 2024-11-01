require "./frames"
require "./types"

module AMQ
  module Protocol
    module V1
      struct Open < AMQP
        getter containerId : String
        getter hostname : String
        getter maxFrameSize : UInt
        getter channelMax : UShort
        getter maxIdleTimeout : Milliseconds
        getter outGoingLocales : Array(IETFLanguageTag)
        getter incomingGoingLocales : Array(IETFLanguageTag)
        getter offeredCapabilities : Array(Symbol)
        getter desiredCapabilities : Array(Symbol)
        getter properties : Fields
      end
    end
  end
end
