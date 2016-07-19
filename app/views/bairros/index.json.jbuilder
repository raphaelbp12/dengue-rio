json.array!(@bairros) do |bairro|
  json.extract! bairro, :id, :nome, :lat, :long
  json.url bairro_url(bairro, format: :json)
end
