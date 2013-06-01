(* Miscellaneous ***********************)

let finally_ f c =
  try let ret = f () in c (); ret
  with exn -> c (); raise exn

let base64 str =
  (* the encoder is consumed by its use, so we have to recreated *)
  let b64_encoder = Cryptokit.Base64.encode_compact_pad () in
  Cryptokit.transform_string b64_encoder str

let base64_decoder str =
  let b64_decoder = Cryptokit.Base64.decode () in
  Cryptokit.transform_string b64_decoder str

let colon_space (k, v) = k ^ ": " ^ v

let encode_url = Uri.pct_encode

let encode_key_equals_value kvs =
  List.map (
    fun (k,v) ->
      (encode_url k) ^ "=" ^ (encode_url v)
  ) kvs

let file_size path =
  let s = Unix.stat path in
  s.Unix.st_size

let xml_declaration = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"

let sort_assoc_list kv_list =
  List.sort (fun (k1,_) (k2,_) -> String.compare k1 k2) kv_list

let getenv_else_exit k =
  try
    Unix.getenv k
  with Not_found ->
    failwith (Printf.sprintf "environment variable %S not set\n%!" k)

open Creds
let creds_of_env () = {
  aws_access_key_id = getenv_else_exit "AWS_ACCESS_KEY_ID";
  aws_secret_access_key = getenv_else_exit "AWS_SECRET_ACCESS_KEY"
}

module C = CalendarLib.Calendar
module P = CalendarLib.Printer.CalendarPrinter

(* parse string of the format ["2009-12-04T22:33:47.279Z"], or ["Tue,
   30 Nov 2010 05:02:47 GMT"]; return seconds since Unix epoch. *)
let unixfloat_of_amz_date_string str =
  try
    let year, month, day, hour, minute, second, millisecond =
      Scanf.sscanf str "%d-%d-%dT%d:%d:%d.%dZ" (
        fun year month day hour minute second millisecond ->
          year, month, day, hour, minute, second, millisecond
      )
    in
    let z = C.make year month day hour minute second in
    let t = C.to_unixfloat z  in
    let millis = (float_of_int millisecond) /. 1000. in
    t +. millis

  with Scanf.Scan_failure _ ->
    (* make a second attempt, now with a different format. ugh *)
    C.to_unixfloat (P.from_fstring "%A, %d %b %Y %H:%M:%S GMT" str)

let amz_date_string_of_unixfloat f =
  let dt = P.sprint "%FT%T" (C.from_unixfloat f) in
  (* add trailing millis and 'Z' *)
  let millis = truncate ((f -. (float_of_int (truncate f))) *. 1000.) in
  Printf.sprintf "%s.%dZ" dt millis


let date_string_of_unixfloat f =
  P.sprint "%F %T%z" (C.from_unixfloat f)

let minutes_from_now minutes =
  let now = Unix.gettimeofday () in
  let seconds_from_now = minutes * 60 in
  let now_plus_minutes = now +. (float_of_int seconds_from_now) in
  amz_date_string_of_unixfloat now_plus_minutes

let now_as_string () =
  amz_date_string_of_unixfloat (Unix.gettimeofday ())

let list_map_i f list =
  let rec loop f j accu = function
    | [] -> List.rev accu
    | h :: t ->
      let m = f j h in
      loop f (j+1) (m::accu) t
  in loop f 0 [] list

(* From OCaml source tree: utils/misc.ml *)
let string_of_file ic =
  let b = Buffer.create 0x10000 in
  let buff = String.create 0x1000 in
  let rec copy () =
    let n = input ic buff 0 0x1000 in
    if n = 0 then Buffer.contents b else
      (Buffer.add_substring b buff 0 n; copy())
  in copy()

let string_of_path path =
  let ic = open_in path in
  finally_ (fun () -> string_of_file ic) (fun () -> close_in ic)

type http_method = [ `GET | `PUT | `HEAD | `DELETE | `POST ]

let string_of_http_method = function
  | `GET -> "GET"
  | `PUT -> "PUT"
  | `HEAD -> "HEAD"
  | `DELETE -> "DELETE"
  | `POST -> "POST"

let filter_map_rev f l =
  let rec aux acc = function
    | [] -> acc
    | x::xs ->
      try
        match f x with
        | Some y -> aux (y::acc) xs
        | None -> aux acc xs
      with _ -> aux acc xs in
  aux [] l

let filter_map f l = List.rev (filter_map_rev f l)

let option_map f o =
  match o with
  | None -> None
  | Some x -> Some (f x)

let option_bind f o =
  match o with
  | None -> None
  | Some x -> f x


let make tm_year tm_mon tm_mday tm_hour tm_min tm_sec _ =
  let tm_wday,tm_yday,tm_isdst = 0,0,false in
  Unix.(
    let tm = {tm_year;tm_mon;tm_mday;tm_hour;tm_min;tm_sec;tm_wday;tm_yday;tm_isdst} in
    Int64.of_float (fst (mktime tm))
  )

let signed_request
    ?(region="")
    ?(http_method=`POST)
    ?(http_uri="/")
    ?expires_minutes
    ~service
    ~creds
    ~params () =

  let http_host = service ^ (if region <> "" then "." ^ region else "") ^ ".amazonaws.com" in
  let params =
    ("Version", ["2009-04-15"] ) ::
      ("SignatureVersion", ["2"]) ::
      ("SignatureMethod", ["HmacSHA1"]) ::
      ("AWSAccessKeyId", [creds.aws_access_key_id]) ::
      params
  in
  let params =
    match expires_minutes with
      | Some i -> ("Expires", [minutes_from_now i]) :: params
      | None -> ("Timestamp", [now_as_string ()]) :: params
  in
  (* sorting the params assoc list *)
  let params = sort_assoc_list params in

  let signature =
    let uri_query_component = Uri.encoded_of_query params in
    let string_to_sign = String.concat "\n" [
      string_of_http_method http_method ;
      String.lowercase http_host ;
      http_uri ;
      uri_query_component
    ]
    in

    let hmac_sha1_encoder = Cryptokit.MAC.hmac_sha1 creds.aws_secret_access_key in
    let signed_string = Cryptokit.hash_string hmac_sha1_encoder string_to_sign in
    base64 signed_string
  in
  let params = ("Signature", [signature]) :: params in
  (http_host ^ http_uri), params


(* Copyright (c) 2011, barko 00336ea19fcb53de187740c490f764f4 All
   rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

   1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the
   distribution.

   3. Neither the name of barko nor the names of contributors may be used
   to endorse or promote products derived from this software without
   specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*)

