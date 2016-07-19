class CreateCasos < ActiveRecord::Migration
  def change
    create_table :casos do |t|
      t.string :dia
      t.integer :qtd
      t.string :bairro

      t.timestamps null: false
    end
  end
end
