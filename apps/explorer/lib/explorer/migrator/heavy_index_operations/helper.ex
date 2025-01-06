defmodule Explorer.Migrator.HeavyIndexOperation.Helper do
  @moduledoc """
  Common functions for Explorer.Migrator.HeavyIndexOperation.* modules
  """

  require Logger

  alias Ecto.Adapters.SQL

  @doc """
  Checks the status of DB index creation
  """
  @spec check_index_creation_status(String.t()) :: :finished | :in_progress | :unknown
  def check_index_creation_status(index_name) do
    check_index_creation_query = """
    SELECT pg_index.indisvalid
    FROM pg_class, pg_index
    WHERE pg_index.indexrelid = pg_class.oid
    AND relname = '#{index_name}';
    """

    case SQL.query(Repo, check_index_creation_query, []) do
      {:ok, %Postgrex.Result{command: :select, columns: ["indisvalid"], rows: [[true]]}} ->
        :finished

      {:ok, _} ->
        :in_progress

      {:error, error} ->
        Logger.error("Failed to check index status: #{inspect(error)}")
        :unknown
    end
  end
end
