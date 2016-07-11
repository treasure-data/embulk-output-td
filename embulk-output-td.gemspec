
Gem::Specification.new do |spec|
  spec.name          = "embulk-output-td"
  spec.version       = "0.3.6"
  spec.authors       = ["Muga Nishizawa"]
  spec.summary       = %[TreasureData output plugin for Embulk]
  spec.description   = %[TreasureData output plugin is an Embulk plugin that loads records to TreasureData read by any input plugins. Search the input plugins by 'embulk-output' keyword.]
  spec.email         = ["muga.nishizawa@gmail.com"]
  spec.licenses      = ["Apache 2.0"]
  spec.homepage      = "https://github.com/treasure-data/embulk-output-td"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
