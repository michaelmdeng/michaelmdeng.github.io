# coding: utf-8

Gem::Specification.new do |spec|
  spec.name          = "midenco"
  spec.version       = "0.1.0"
  spec.authors       = ["michael"]
  spec.email         = ["michael@miden.co"]

  spec.summary       = "Theme for miden.co"
  spec.description   = "Theme for miden.co"
  spec.homepage      = "https://github.com/michaelmdeng/michaelmdeng.github.io"
  spec.license       = "MIT"

  spec.files         = Dir.glob("**/{*,.*}").select do |f|
    f.match(%r{^(assets/(js|css|fonts|data)|_(includes|layouts|sass)/|(LICENSE|README.md))}i)
  end

  spec.required_ruby_version = '~> 2.1'
    
  spec.add_runtime_dependency "jekyll", "~> 3.3"
  spec.add_runtime_dependency "jekyll-paginate", "~> 1.1"
  spec.add_runtime_dependency "jekyll-seo-tag", "~> 2.3"

  spec.add_development_dependency "bundler", "~> 1.12"
  spec.add_development_dependency "rake", "~> 10.0"

end
