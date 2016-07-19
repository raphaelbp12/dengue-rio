class Caso < ActiveRecord::Base

  def self.filter(startdate, enddate, bairro, total)

    startdate_str = ''
    enddate_str = ''
    bairro_str = ''

    if startdate != ''
      startdate_str = "dia >= "+startdate.to_s
      if enddate != ''
        enddate_str = " AND "
      end
    end

    if enddate != ''
      enddate_str = enddate_str + "dia <= "+enddate.to_s
    end

    if bairro != ''
      if (startdate != '') || (enddate != '')
        bairro_str = " AND "
      end

      bairro_str = bairro_str + "bairro in ('"+bairro.gsub(',',"','")+"')"
    end


    if total
      filter_str = startdate_str+enddate_str
      casos = Caso.where(filter_str).sum(:qtd)
    else
      filter_str = startdate_str+enddate_str+bairro_str
      casos = Caso.where(filter_str).order("dia ASC").group(:dia).sum(:qtd)
    end
    return casos
  end

  def self.lista_casos_bairro
    Caso.group(:bairro).order('sum_qtd DESC').sum(:qtd)
  end

  def self.lista_bairro
    Caso.distinct.pluck(:bairro)
  end

  def self.lista_casos_bairro_data(startdate, enddate)
    startdate_str = ''
    enddate_str = ''

    if startdate != ''
      startdate_str = "dia >= "+startdate.to_s
      if enddate != ''
        enddate_str = " AND "
      end
    end

    if enddate != ''
      enddate_str = enddate_str + "dia <= "+enddate.to_s
    end

    filter_str = startdate_str+enddate_str

    Caso.where(filter_str).group(:bairro).order('sum_qtd DESC').sum(:qtd)
  end
end
