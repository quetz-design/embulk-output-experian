
Gem::Specification.new do |spec|
  spec.name          = "embulk-output-experian"
  spec.version       = "0.1.1.4"
  spec.authors       = ["Kouta Yoshimura"]
  spec.summary       = "Experian output plugin for Embulk"
  spec.description   = "Upload records and reserve mail delivery with Experian."
  spec.email         = ["quetz.design@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/quetz.design/embulk-output-experian"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'httpclient'
  spec.add_development_dependency 'embulk', ['>= 0.8.13']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
