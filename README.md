# ActiveRecord Spanner adapter

The [Cloud Spanner](https://cloud.google.com/spanner/) adapter for ActiveRecord.

## Status
Proof of concept.
You cannot expect that this gem is ready for production use -- many features are not supported.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'activerecord-spanner-adapter',
  git: 'https://github.com/supership-jp/activerecord-spanner-adapter.git'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install activerecord-spanner-adapter

## Usage

Add a configuration like this into your `database.yml`.

```yaml
default:
  adapter: spanner
  project: your-gcp-project-name
  instance: your-spanner-instance-name
  database: your-spanner-database-name
  keyfile: path/to/serivce-account-credential.json
```

*NOTE*: This adapter uses UUIDs as primary keys by default unlike other adapters.
This is because monotonically increasing primary key restricts write performance in Spanner.

c.f. https://cloud.google.com/spanner/docs/best-practices


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/supership-jp/activerecord-spanner-adapter.

## License
Copyright (c) 2017 Supership Inc.

Licensed under MIT license.

