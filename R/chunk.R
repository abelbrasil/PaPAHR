
#' Organizes DBC files to be downloaded into groups
#'
#' @description Obtém os nomes dos arquivos DBC em `files_name` e os divide em grupos (chunks). O número de grupos e a quantidade de arquivos em cada grupo dependem do número de meses especificado.
#'
#' @param files_name Um vetor contendo os nomes dos arquivos.
#' @param data_type String. O padrão é NULL. Valor aceito "PA" ou "SP".
#'
#' @return files_chunks, uma lista de vetores de strings.
#'
#' @examples \dontrun{chunk( files_name = files_name)}
#'
#' @export
chunk <- function(files_name,data_type=NULL){

  `%>%` <- dplyr::`%>%`

  #files_name <- dplyr::pull(dir_files, file_name)

  if (is.null(data_type)){
    peso = 0.15
  } else if(data_type == 'PA' | data_type == 'SP'){
    peso = 0.09
  }

  n_files <- files_name %>% length()
  chunk_size <- ceiling(peso*n_files)
  n_chunks <- ceiling(n_files/chunk_size)

  files_chunks <- list()
  n = 1

  #Distribuir os arquivos em cada chunk
  while (n <= n_chunks) {
    upper_limit <- ifelse(n*chunk_size > n_files, n_files, n*chunk_size)
    files_chunks[[n]] <- files_name[(chunk_size*(n-1)+1):upper_limit]
    names(files_chunks)[n] = stringr::str_glue("chunk_{n}")
    n = n + 1
  }

  return(files_chunks)
}
