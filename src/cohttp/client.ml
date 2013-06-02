module C = Cohttp
module CU = Cohttp_lwt_unix
module CB = Cohttp_lwt_body
module CUC = Cohttp_lwt_unix.Client
module CUR = Cohttp_lwt_unix.Response

let (>>=) = Lwt.bind

module Opt = struct
  let unopt = function Some v -> v | None -> raise Not_found
  let default d = function Some v -> v | None -> d
  let map f = function Some v -> Some (f v) | None -> None
end

type headers = (string * string) list

type request_body =  [ `InChannel of int * Lwt_io.input_channel | `String of string ]

exception Http_error of (int * headers * string)

let post_put f ?headers ?body uri_string =
  lwt body = match body with
    | Some req_body ->
      (match req_body with
        | `InChannel (len, ic) ->
          Lwt_io.read ~count:len ic >>= fun body ->
          if String.length body = len
          then Lwt.return (CB.body_of_string body)
          else Lwt.return None
        | `String s -> Lwt.return (CB.body_of_string s))
    | None -> Lwt.return None
  in
  let headers = Opt.map (C.Header.of_list) headers in
  let headers = match headers with
    | Some h -> Some (C.Header.add h "Content-Type" "application/x-www-form-urlencoded; charset=utf8")
    | None -> Some (C.Header.init_with "Content-Type" "application/x-www-form-urlencoded; charset=utf8")
  in
  lwt body_string = (CB.string_of_body body) in
  let () = Printf.printf "DEBUG: post_put: sent body =\n%s\n%!" body_string in
  let uri = Uri.of_string uri_string in
  lwt resp = f ?body ?headers uri in
  let resp, body = Opt.unopt resp in
  let resp_hdrs = CUR.headers resp in
  lwt body_string = CB.string_of_body body in
  let () = Printf.printf "DEBUG: post_put: received body = \n%s\n%!" body_string in
  Lwt.return (C.Header.to_list resp_hdrs, body_string)

let post = post_put (CUC.post ~chunked:false)
let put  = post_put (CUC.put ~chunked:false)

let get_head_delete f ?headers uri_string =
  let headers = Opt.map (C.Header.of_list) headers in
  let uri = Uri.of_string uri_string in
  lwt resp = f ?headers uri in
  let resp, body = Opt.unopt resp in
  let resp_hdrs = CUR.headers resp in
  lwt body_string = CB.string_of_body body in
  Lwt.return (C.Header.to_list resp_hdrs, body_string)

let get    = get_head_delete CUC.get
let head   = get_head_delete CUC.head
let delete = get_head_delete CUC.delete

let get_to_chan ?headers uri_string oc =
  lwt headers, body_string = get ?headers uri_string in
  Lwt_io.write oc body_string >> Lwt.return headers
