defmodule Explorer.Migrator.HeavyIndexOperation do
  @moduledoc """
  Provides a template for making heavy DB operations such as creation/deletion of new indexes in the large tables
  with tracking status of those migrations.
  """

  @doc """
  This callback returns the name of the migration. The name is used to track the operation's status in
  `Explorer.Migrator.MigrationStatus`.
  """
  @callback migration_name :: String.t()

  @doc """
  This callback returns the string with a psql query to initialize DB operation like creation or deletion of the index.
  """
  @callback init_query :: String.t()

  @doc """
  This callback checks DB index operation (creation or deletion) status.
  """
  @callback check_index_operation_status() :: :finished | :in_progress | :unknown

  @doc """
    This callback updates the migration completion status in the cache.

    The callback is invoked in two scenarios:
    - When the migration is already marked as completed during process initialization
    - When the migration finishes processing all entities

    The implementation updates the in-memory cache that tracks migration completion
    status, which is used during application startup and by performance-critical
    operations to quickly determine if specific data migrations have been completed.
    Some migrations may not require cache updates if their completion status does not
    affect system operations.

    ## Returns
    N/A
  """
  @callback update_cache :: any()

  defmacro __using__(_opts) do
    quote do
      @behaviour Explorer.Migrator.HeavyIndexOperation

      use GenServer, restart: :transient

      import Ecto.Query

      alias Ecto.Adapters.SQL
      alias Explorer.Migrator.MigrationStatus
      alias Explorer.Repo

      def start_link(_) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
      end

      @spec migration_finished? :: boolean()
      def migration_finished? do
        MigrationStatus.get_status(migration_name()) == "completed"
      end

      @impl true
      def init(_) do
        {:ok, %{}, {:continue, :ok}}
      end

      @impl true
      def handle_continue(:ok, state) do
        case MigrationStatus.fetch(migration_name()) do
          %{status: "completed"} ->
            update_cache()
            {:stop, :normal, state}

          migration_status ->
            MigrationStatus.set_status(migration_name(), "started")
            SQL.query!(Repo, init_query(), [])
            schedule_next_status_check()
            {:noreply, (migration_status && migration_status.meta) || %{}}
        end
      end

      @impl true
      def handle_info(:check_index_operation_status, state) do
        case check_index_operation_status() do
          :finished ->
            update_cache()
            MigrationStatus.set_status(migration_name(), "completed")
            {:stop, :normal, state}

          _ ->
            schedule_next_status_check()

            {:noreply, state}
        end
      end

      @spec run_task() :: any()
      defp run_task, do: Task.async(fn -> check_index_operation_status() end)

      defp schedule_next_status_check(timeout \\ nil) do
        Process.send_after(
          self(),
          :check_index_operation_status,
          timeout || Application.get_env(:explorer, __MODULE__)[:timeout] || :timer.minutes(10)
        )
      end
    end
  end
end
