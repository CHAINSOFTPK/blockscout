defmodule Explorer.Migrator.HeavyIndexOperations.AddLogsBlockHashIndex do
  @moduledoc """
  Add B-tree index on `logs` table for `block_hash` column.
  """

  use Explorer.Migrator.HeavyIndexOperation

  alias Explorer.Chain.Cache.BackgroundMigrations
  alias Explorer.Migrator.HeavyIndexOperation
  alias Explorer.Migrator.HeavyIndexOperation.Helper, as: HeavyIndexOperationHelper

  @migration_name "heavy_indexes_add_logs_block_hash_index"
  @index_name "logs_block_hash_index"

  @impl HeavyIndexOperation
  def migration_name, do: @migration_name

  @impl HeavyIndexOperation
  def init_query do
    """
      CREATE INDEX CONCURRENTLY #{@index_name} on logs (block_hash);
    """
  end

  @impl HeavyIndexOperation
  def check_index_operation_status do
    HeavyIndexOperationHelper.check_index_creation_status(@index_name)
  end

  @impl HeavyIndexOperation
  def update_cache do
    BackgroundMigrations.set_heavy_indexes_add_logs_block_hash_index_finished(true)
  end
end
