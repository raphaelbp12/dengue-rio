require "sqlite3"
require "csv"

namespace :denguerio do
  desc "TODO"
  task importdb: :environment do

    db = SQLite3::Database.new "dados.db"

    db.execute( "select * from casos" ) do |row|
      p row

      a = row[1].split(/\//)
      data = a[2].to_s+a[1].to_s+a[0].to_s

      Caso.create!(dia: data, qtd: row[2], bairro: row[3])
    end
  end

  task importbairros: :environment do
    csv_text = File.read('bairros.csv')
    csv = CSV.parse(csv_text, :headers => true)
    csv.each do |row|
      p row[0]
      Bairro.create!(nome: row[0], lat: row[1], long: row[2])
    end
  end

end
