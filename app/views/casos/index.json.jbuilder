json.array!(@casos) do |caso|
  json.extract! caso, :id, :dia, :qtd, :bairro
  json.url caso_url(caso, format: :json)
end
