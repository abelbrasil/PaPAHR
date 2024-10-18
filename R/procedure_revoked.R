
#' Retorna informações sobre procedimentos que foram revogados
#'
#' @description Alguns procedimentos são revogados e, a partir do mês da revogação, as bases do SIGTAP deixam de fornecer informações sobre eles. Para acessar os dados desses procedimentos, é necessário consultar os meses anteriores à revogação. Esta função busca os valores faltantes, e, para que ela funcione corretamente, o usuário deve informar um intervalo de tempo que inclua o(s) procedimento(s) antes da revogação.
#'
#' @param year_start numeric. Ano inicial para o download dos dados, no formato yyyy.
#' @param month_start numeric. Mês inicial para o download dos dados, no formato mm.
#' @param year_end numeric. Ano final para o download dos dados, no formato yyyy.
#' @param month_end numeric. Mês final para o download dos dados, no formato mm.
#' @param dados É o conjunto de dados retornado por uma das funções principais.
#' @param proc_sec Lógico. O valor padrão é FALSE. Esse parâmetro só deve ser igual a TRUE quando os procedimentos revogados forem os procedimentos secundários. Esse tipo de procedimento só aparece quando são utilizadas as funções do Serviço Profissional (SP).
#'
#' @return A função retorna um subconjunto da base de dados de entrada, contendo exclusivamente as linhas nas quais a coluna 'Procedimentos realizados' possuía valores nulos. Os valores nulos nestas linhas são imputados com os valores correspondentes.
#'
#' @examples
#'   \dontrun{
#'     dados = procedure_revoked(
#'       year_start = 2023,
#'       month_start = 1,
#'       year_end = 2023,
#'       month_end = 3,
#'       dados = outputSIH_RD,
#'       proc_sec = FALSE
#'     )
#'   }
#'
#' @export
procedure_revoked <-
  function(year_start,
           month_start,
           year_end,
           month_end,
           dados,
           proc_sec = FALSE) {

  `%>%` <- dplyr::`%>%`

  download_sigtap_files(year_start,
                        month_start,
                        year_end,
                        month_end,
                        newer = FALSE)

  procedure_details <- get_procedure_details()


  if (!proc_sec) {
    #Filtra só as linhas que tem valores nulos na coluna "Procedimentos realizados"
    dados_na = dados[is.na(dados$`Procedimentos realizados`), ]

    #Filtra e renomeia apenas as colunas que serão utilizadas.
    procedure_details <-
      procedure_details %>% dplyr::select(
        CO_PROCEDIMENTO,
        NO_PROCEDIMENTO,
        NO_GRUPO,
        NO_SUB_GRUPO,
        NO_FORMA_ORGANIZACAO,
        COMPLEXIDADE,
        NO_FINANCIAMENTO,
        NO_SUB_FINANCIAMENTO
      ) %>%  dplyr::rename(
        `Procedimentos realizados` = NO_PROCEDIMENTO,
        `Grupo de Procedimentos` = NO_GRUPO,
        `SubGrupo de Procedimentos` = NO_SUB_GRUPO,
        `Forma de organizacao` = NO_FORMA_ORGANIZACAO,
        `Complexidade do Procedimento` = COMPLEXIDADE,
        `Financiamento` = NO_FINANCIAMENTO,
        `SubTp FAEC` = NO_SUB_FINANCIAMENTO
      )

    #Obtêm a posição da coluna "Procedimentos realizados"
    position = which(names(dados_na) == "Procedimentos realizados")

    #Remove as colunas que tem valores nulos
    dados_na = dados_na %>% dplyr::select(
      -`Procedimentos realizados`,-`Grupo de Procedimentos`,-`SubGrupo de Procedimentos`,-`Forma de organizacao`,-`Complexidade do Procedimento`,-`Financiamento`,-`SubTp FAEC`
    )


    output = dados_na %>% dplyr::left_join(
      procedure_details %>% dplyr::distinct(CO_PROCEDIMENTO, .keep_all = TRUE),
      c("CO Procedimentos realizados" = "CO_PROCEDIMENTO")
    )
    colunas_para_mover <- c(
      "Procedimentos realizados",
      "Grupo de Procedimentos",
      "SubGrupo de Procedimentos",
      "Forma de organizacao",
      "Complexidade do Procedimento",
      "Financiamento",
      "SubTp FAEC"
    )
  }
  else {
    #Filtra só as linhas que tem valores nulos na coluna "Procedimentos Secundarios"
    dados_na = dados[is.na(dados$`Procedimentos Secundarios`), ]

    #Filtra e renomeia apenas as colunas que serão utilizadas.
    procedure_details <-
      procedure_details %>% dplyr::select(
        CO_PROCEDIMENTO,
        NO_PROCEDIMENTO,
        NO_GRUPO,
        NO_SUB_GRUPO,
        NO_FORMA_ORGANIZACAO,
        COMPLEXIDADE,
        NO_FINANCIAMENTO,
        NO_SUB_FINANCIAMENTO
      ) %>%  dplyr::rename(
        `Procedimentos Secundarios` = NO_PROCEDIMENTO,
        `Grupo Proc. Secundario` = NO_GRUPO,
        `Sub-grupo Proc. Secundario` = NO_SUB_GRUPO,
        `Forma de organizacao Secundaria` = NO_FORMA_ORGANIZACAO,
        `Complexidade do ato profissional` = COMPLEXIDADE,
        `Tipo de financiamento do ato profissional` = NO_FINANCIAMENTO,
        `Subtipo FAEC do ato profissional` = NO_SUB_FINANCIAMENTO
      )

    #Obtêm a posição da coluna "Procedimentos Secundarios"
    position = which(names(dados_na) == "Procedimentos Secundarios")

    #Remove as colunas que tem valores nulos
    dados_na = dados_na %>% dplyr::select(
      -`Procedimentos Secundarios`,
      -`Grupo Proc. Secundario`,
      -`Sub-grupo Proc. Secundario`,
      -`Forma de organizacao Secundaria`,
      -`Complexidade do ato profissional`,
      -`Tipo de financiamento do ato profissional`,
      -`Subtipo FAEC do ato profissional`
    )


    output = dados_na %>% dplyr::left_join(
      procedure_details %>% dplyr::distinct(CO_PROCEDIMENTO, .keep_all = TRUE),
      c("CO Procedimentos Secundarios" = "CO_PROCEDIMENTO")
    )

    colunas_para_mover <- c(
      "Procedimentos Secundarios",
      "Grupo Proc. Secundario",
      "Sub-grupo Proc. Secundario",
      "Forma de organizacao Secundaria",
      "Complexidade do ato profissional",
      "Tipo de financiamento do ato profissional",
      "Subtipo FAEC do ato profissional"
    )
  }

  #No join acima, novas colunas foram adicionadas. Agora, vamos ordenar essas novas colunas.
  outras_colunas <- setdiff(names(output), colunas_para_mover)
  nova_ordem <- c(outras_colunas[1:(position - 1)],
                  colunas_para_mover,
                  outras_colunas[position:length(outras_colunas)]
                  )
  output_reordenado <- output[,nova_ordem]

  return(output_reordenado)
  }





