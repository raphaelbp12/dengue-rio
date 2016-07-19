def generateInsert(data, qtd, bairro):
    qry = "INSERT INTO casos (dia, qtd, bairro) VALUES ('" + data + "', " + str(qtd) + ", '" + bairro + "')"
    return qry