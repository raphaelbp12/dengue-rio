require 'test_helper'

class CasosControllerTest < ActionController::TestCase
  setup do
    @caso = casos(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:casos)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create caso" do
    assert_difference('Caso.count') do
      post :create, caso: { bairro: @caso.bairro, dia: @caso.dia, qtd: @caso.qtd }
    end

    assert_redirected_to caso_path(assigns(:caso))
  end

  test "should show caso" do
    get :show, id: @caso
    assert_response :success
  end

  test "should get edit" do
    get :edit, id: @caso
    assert_response :success
  end

  test "should update caso" do
    patch :update, id: @caso, caso: { bairro: @caso.bairro, dia: @caso.dia, qtd: @caso.qtd }
    assert_redirected_to caso_path(assigns(:caso))
  end

  test "should destroy caso" do
    assert_difference('Caso.count', -1) do
      delete :destroy, id: @caso
    end

    assert_redirected_to casos_path
  end
end
