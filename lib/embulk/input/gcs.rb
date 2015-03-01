Embulk::JavaPlugin.register_input(
  :gcs, "org.embulk.input.gcs.GcsFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
