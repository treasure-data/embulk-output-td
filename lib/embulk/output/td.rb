Embulk::JavaPlugin.register_output(
  "td", "org.embulk.output.TDOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
