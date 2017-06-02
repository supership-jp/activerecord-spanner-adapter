module ActiveRecord
  module ConnectionAdapters
    module Spanner
      class NullTransaction < ConnectionAdapters::NullTransaction
        def initialize(client)
          super()
          @client = client
        end

        attr_reader :client  # :nodoc:

        delegate :commit, :execute, to: :client
      end

      class SpannerTransaction < ConnectionAdapters::Transaction
        def initialize(connection, client, deadline: nil, run_commit_callbacks: false, **options)
          super(connection, options, run_commit_callbacks: run_commit_callbacks)
          @client = client
          @enum = @client.enum_for(:transaction, deadline: deadline)
          @tx = @enum.peek
        end

        attr_reader :tx

        delegate :execute, to: :tx

        def commit
          tx.commit do |c|
            yield c if block_given?
          end
          super()
        end

        def rollback
          tx.safe_rollback
          super
        end
      end

      class SpannerSnapshot < ConnectionAdapters::Transaction
        def initialize(connection, client, snapshot: nil, run_commit_callbacks: false, **options)
          super(connection, options, run_commit_callbacks: run_commit_callbacks)
          @client = client
          @enum = @client.enum_for(:snapshot, **options)
          @tx = @enum.peek
        end

        attr_reader :tx

        delegate :execute, to: :tx
      end

      class TransactionManager < ConnectionAdapters::TransactionManager
        def initialize(connection, client)
          super(connection)
          @client = client
          @null_transaction = NullTransaction.new(client)
        end

        def begin_transaction(deadline: nil, snapshot: nil, **options)
          @connection.lock.synchronize do
            run_commit_callbacks = !current_transaction.joinable?
            tx = 
              if snapshot
                SpannerSnapshot.new(
                  @connection, @client,
                  snapshot: snapshot, run_commit_callbacks: run_commit_callbacks,
                  **options
                )
              else
                SpannerTransaction.new(
                  @connection, @client,
                  deadline: deadline, run_commit_callbacks: run_commit_callbacks,
                  **options
                )
              end
            @stack.push(tx)
            tx
          end
        end

        def current_transaction
          @stack.last || @null_transaction
        end
      end
    end
  end
end
