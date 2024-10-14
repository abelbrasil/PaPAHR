
#' Downloads DBC data from the specified healthcare systems: 'SIA-PA', 'SIH-RD', 'SIH-RJ', or 'SIH-SP'
#'
#' @description Realiza o download de arquivos DBC dos sistemas de saúde especificados ('SIA-PA', 'SIH-RD', 'SIH-RJ' ou 'SIH-SP') e os salva em um diretório definido pelo usuário, criando automaticamente uma pasta chamada 'file_DBC'.
#'
#' @param information_system String. Valor aceito "SIA-PA", "SIH-RD", "SIH-RJ" ou "SIH-SP".
#' @param year_start numeric. Ano inicial para o download dos dados, no formato yyyy.
#' @param month_start numeric. Mês inicial para o download dos dados, no formato mm.
#' @param year_end numeric. Ano final para o download dos dados, no formato yyyy.
#' @param month_end numeric. Mês final para o download dos dados, no formato mm.
#' @param state_abbr string ou vetor de strings. Sigla da Unidade Federativa
#' @param save_path String. Diretório onde os arquivos DBC serão salvos. O padrão é ".\\".
#'
#' @examples
#'   download_dbc(
#'     information_system = "SIA-PA",
#'     year_start = 2023,
#'     month_start = 1,
#'     year_end = 2023,
#'     month_end = 3,
#'     state_abbr = "CE",
#'     save_path = ".\\"
#'   )
#'
#' @export
download_dbc <-
  function(information_system,
           year_start,
           month_start,
           year_end,
           month_end,
           state_abbr,
           save_path = ".\\") {

    information_system = toupper(trimws(information_system))
    state_abbr = toupper(trimws(state_abbr))

    data_source = stringr::str_sub(information_system, 1, 3)
    data_type = stringr::str_sub(information_system, 5, 6)

    publication_date_start <-
      lubridate::ym(stringr::str_glue("{year_start}-{month_start}"))
    publication_date_end <-
      lubridate::ym(stringr::str_glue("{year_end}-{month_end}"))

    #Lista os nomes dos arquivos que serão baixados de cada mês
    dir_files = list_SIA_SIH_files(data_source,
                                   data_type,
                                   state_abbr,
                                   publication_date_start,
                                   publication_date_end)

    #Verifica se dir_files contém o nome de pelo menos um arquivo data_type para cada mês.
    check_file_list(dir_files,
                    data_type,
                    state_abbr,
                    publication_date_start,
                    publication_date_end)

    #Separa os arquivos em grupos, caso haja vários arquivos para serem baixados.
    files_chunks = chunk(dir_files$file_name)
    n_chunks = length(files_chunks)

    base_url <- stringr::str_glue("ftp://ftp.datasus.gov.br/dissemin/publicos/{data_source}SUS/200801_/Dados/")

    #Criando as pastas
    output_dir <- stringr::str_glue("{save_path}\\file_DBC")
    if (!dir.exists(output_dir)) {
      dir.create(output_dir)
    }
    output_dir <- stringr::str_glue("{save_path}\\file_DBC\\{information_system}")
    if (!dir.exists(output_dir)) {
      dir.create(output_dir)
    }

    for (n in 1:n_chunks) {
      download_files_url <-
        stringr::str_glue("{base_url}{files_chunks[[n]]}")
      output_files_path <-
        stringr::str_glue("{save_path}\\file_DBC\\{information_system}\\{files_chunks[[n]]}")

      #Download dos dados
      purrr::walk2(download_files_url, output_files_path, curl::curl_download)
    }
  }
