class WelcomeController < ApplicationController
  def index

    params[:startdate] ||= ''
    params[:enddate] ||= ''
    params[:bairro] ||= ''

    @startdate = params[:startdate]
    @enddate = params[:enddate]
    @bairro = params[:bairro]



    @startdatevalue = params[:startdate][0..3].to_s+"-"+params[:startdate][4..5].to_s+"-"+params[:startdate][6..7].to_s
    @enddatevalue = params[:enddate][0..3].to_s+"-"+params[:enddate][4..5].to_s+"-"+params[:enddate][6..7].to_s

    @data = Caso.filter(params[:startdate],params[:enddate],params[:bairro], false)
    @lista = Caso.lista_casos_bairro_data(params[:startdate],params[:enddate])

    @total = Caso.filter(params[:startdate],params[:enddate],params[:bairro], true)
    @total_bairro = Caso.filter(params[:startdate],params[:enddate],params[:bairro], true)

    @bairroobject = Bairro.find_by(nome: @bairro)

    if @bairro == ''
      @zoom = 10
      @bairronome = "Rio de Janeiro"
    else
      @zoom = 13
      @bairronome = @bairro
    end
  end
end
