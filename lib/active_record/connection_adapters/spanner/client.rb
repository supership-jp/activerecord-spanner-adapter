module ActiveRecord
  module ConnectionAdapters
    module Spanner
      # raised on unsupported operations in a certain transaction state.
      class TransactionStateError < ActiveRecordError
      end

      class InitialPhaseClient
        def initialize(client)
          @client = client
          @next = self
        end

        attr_reader :next

        delegate :insert, :update, :delete, :execute, :close, to: :client

        def begin_transaction
          @next = ReadWriteTransactionClient.begin(client)
        end

        def begin_snapshot(**options)
          @next = SnapshotClient.begin(client, **options)
        end

        %w[ commit rollback ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}
              raise TransactionStateError, 'not in a transaction'
            end
          EOS
        end

        private
        attr_reader :client
      end

      class ReadWriteTransactionClient
        def initialize(client, tx, commit_op, rollback_op)
          @next = self
          @client = client
          @tx = tx

          @commit_op = commit_op
          @rollback_op = rollback_op
        end

        class << self
          def begin(client)
            enum = enum_for(:transaction, client, deadline: 0)
            closer = ->{
              begin
                enum.next
              rescue StopIteration => stop
                stop.result
              end
            }

            begin
              tx, rollback_op = enum.next
              on_rollback = ->{
                begin
                  rollback_op.call
                ensure
                  closer.call
                end
              }
              return ReadPhaseClient.new(client, tx, closer, on_rollback)
            rescue
              closer.call
              raise
            end
          end

          private
          def transaction(client, *args)
            client.transaction(*args) do |tx|
              rollbacked = false
              yield tx, ->{ rollbacked = true }
              raise Google::Cloud::Spanner::Rollback if rollbacked
            end
          end
        end

        attr_reader :next

        delegate :close, to: :client
        delegate :execute, :insert, :update, :delete, to: :tx

        def commit
          result = @commit_op.call
          result.tap do
            @next = InitialPhaseClient.new(@client)
          end
        end

        def rollback
          @rollback_op.call.tap do
            @next = InitialPhaseClient.new(@client)
          end
        end

        %w[ begin_transaction begin_snapshot ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}(*args)
              raise TransactionStateError, 'already in a transaction'
            end
          EOS
        end

        private
        attr_reader :client, :tx  # :nodoc:
      end

      class ReadPhaseClient < ReadWriteTransactionClient
        %w[ insert update delete ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}(*args)
              super.tap { transit_to_write_phase }
            end
          EOS
        end

        private
        def transit_to_write_phase
          @next = WritePhaseClient.new(@client, @tx, @commit_op, @rollback_op)
        end
      end

      class WritePhaseClient < ReadWriteTransactionClient
        def execute(*args)
          raise TransactionStateError, "cannot read after write within a transaction"
        end
      end

      class SnapshotClient
        def initialize(client, **options)
          @client = client

          enum = client.enum_for(:snapshot, **options)
          @snapshot, @closer = enum.next, ->{ loop { enum.next } }
          @next = self
        end

        class << self
          alias begin new
        end

        attr_reader :next

        delegate :close, to: :client
        delegate :execute, to: :snapshot

        %w[ insert update delete ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}(*)
              raise TransactionStateError, "cannot write within a read-only transaction"
            end
          EOS
        end

        %w[ commit rollback ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}
              terminate_phase
              true
            end
          EOS
        end

        %w[ begin_transaction begin_snapshot ].each do |name|
          class_eval(<<-"EOS", __FILE__, __LINE__+1)
            def #{name}(*args)
              raise TransactionStateError, 'already in a transaction'
            end
          EOS
        end

        private
        attr_reader :client, :snapshot

        def terminate_phase
          @next = InitialPhaseClient.new(@client)
        ensure
          @closer.call
        end
      end
    end
  end
end
