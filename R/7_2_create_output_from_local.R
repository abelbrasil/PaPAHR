
#' Create SUS-SIA/SIH database from local DBC files
#'
#' @description Processar arquivos dos sistemas de informação SIA e SIH (DATASUS) que já estão baixados localmente e combiná-los com informações do CNES e SIGTAP.
#'
#' @param state_abbr String. Sigla da Unidade Federativa
#' @param dbc_dir_path Diretório que contêm os arquivos DBC
#' @param county_id Codigo(s) do Municipio de Atendimento. O padrao é NULL.
#' @param health_establishment_id Código(s) do estabelecimento de saúde
#'
#' @examples
#'   \dontrun{
#'     create_output_from_local(
#'       state_abbr = "CE",
#'       dbc_dir_path = "X:/USID/BOLSA_EXTENSAO_2024/dbc/dbc-2301-2306",
#'       county_id = "230440",
#'       health_establishment_id = c("2561492", "2481286")
#'     )
#'   }
#'
#' @export
create_output_from_local <-
  function(state_abbr,
           dbc_dir_path,
           county_id = NULL,
           health_establishment_id = NULL) {

    tempo_inicio <- system.time({

      #Se o id do municipio for igual a 7 caracteres, remove o último caracter.
      if(!is.null(county_id)){
        if (nchar(county_id) == 7) {
          county_id <- substr(county_id, 1, nchar(county_id) - 1)
        }
      }

      state_abbr = toupper(trimws(state_abbr))
      get_counties()
      get_CNES()

      download_sigtap_files() #obtem os dodos do SIGTAP do mes mais recente disponivel.

      outputSIA <- get_datasus_from_local(
        state_abbr,
        county_id,
        dbc_dir_path,
        information_system = "SIA",
        health_establishment_id
      )

      outputSIH_AIH <- get_datasus_from_local(
        state_abbr,
        county_id,
        dbc_dir_path,
        information_system = "SIH-AIH",
        health_establishment_id
      )

      outputSIH_SP <- get_datasus_from_local(
        state_abbr,
        county_id,
        dbc_dir_path,
        information_system = "SIH-SP",
        health_establishment_id
      )

      rm("counties", envir = .GlobalEnv)
      rm("health_establishment", envir = .GlobalEnv)

      write.csv2(outputSIA,
                 "./data-raw/outputSIA.csv",
                 na = "",
                 row.names = FALSE)
      write.csv2(outputSIH_AIH,
                 "./data-raw/outputSIH_AIH.csv",
                 na = "",
                 row.names = FALSE)
      write.csv2(outputSIH_AIH,
                 "./data-raw/outputSIH_SP.csv",
                 na = "",
                 row.names = FALSE)
    })
    cat("Tempo de execução:", tempo_inicio[3]/60, "minutos\n")
    return(list(outputSIA, outputSIH_AIH, outputSIH_SP))
  }





