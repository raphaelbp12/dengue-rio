class CreateBairros < ActiveRecord::Migration
  def change
    create_table :bairros do |t|
      t.string :nome
      t.string :lat
      t.string :long

      t.timestamps null: false
    end
  end
end
