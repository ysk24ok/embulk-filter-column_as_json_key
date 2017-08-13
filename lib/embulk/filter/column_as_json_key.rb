Embulk::JavaPlugin.register_filter(
  "column_as_json_key", "org.embulk.filter.column_as_json_key.ColumnAsJsonKeyFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
