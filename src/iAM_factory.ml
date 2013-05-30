module Make = functor (HC : Aws_sigs.HTTP_CLIENT) ->
struct

open Lwt

module Pcre = Re_pcre
module Util = Aws_util

module C = CalendarLib.Calendar
module P = CalendarLib.Printer.CalendarPrinter

exception Service_down

(* Miscellaneous *****************************************************************)

let host = "sts.amazonaws.com"

let sign_request aws_access_key_id aws_secret_access_key params =
  let signature =
    let sorted_params = Util.sort_assoc_list params in
    let key_equals_value = Util.encode_key_equals_value sorted_params in
    let uri_query_component = String.concat "&" key_equals_value in
    let string_to_sign = String.concat "\n" [
      "POST" ;
      String.lowercase host ;
      "/" ;
      uri_query_component
    ]
    in
    let hmac_sha256_encoder = Cryptokit.MAC.hmac_sha256 aws_secret_access_key in
    let signed_string = Cryptokit.hash_string hmac_sha256_encoder string_to_sign in
    let signed_string = String.sub signed_string 0 ((String.length signed_string) - 0) in
    let b64 = Util.base64 signed_string in
    String.sub b64 0 ((String.length b64) - 0)
  in
  ("Signature", signature)::params


type node =
  | E of string * attr list * node list
  | P of string
and attr = (string * string) * string

let xml_of_string s =
  (* we drop the namespace part of the element here *)
  let el ((ns, name), atts) kids = E (name, atts, kids) in
  let data d = P d in
  let input = Xmlm.make_input ~strip:true (`String (0,s)) in
  let _, node = Xmlm.input_doc_tree ~el ~data input in
  node

exception Invalid_credential

let xml_get_content_of name xml : string option =
  let rec aux = function
    | E (n,_,[P v]) when n=name -> Some v
    | E (_,_,l) ->
      let rec loop = function
        | [] -> None
	| x::xs -> match aux x with
	    | Some x -> Some x
	    | None -> loop xs
      in loop l
    | _ -> None
  in aux xml

type session_token = {
  session_token : string;
  secret_access_key : string;
  access_key_id : string;
  expiration : int64;
}

let token_is_valid token = token.expiration > (Int64.of_float (C.to_unixfloat (C.now ())))

let get_session_token ?duration ?(version="2011-06-15") aws_access_key_id aws_secret_access_key =
  let content =
    let s (n,v)= Some (n,v) in
    let n (n,v)= Util.option_map (fun v -> (n,string_of_int v)) v in
    let now = P.sprint "%FT%T" (C.from_unixfloat (Unix.gettimeofday ())) in
    s("AWSAccessKeyId",aws_access_key_id)
    ::s("Action","GetSessionToken")
    ::n("DurationSeconds",duration)
    ::s("SignatureMethod","HmacSHA256")
    ::s("SignatureVersion","2")
    ::s("Timestamp",now )
    ::s("Version",version)
    ::[] in
  let params = Util.filter_map (fun x -> x) content in
  let params = sign_request aws_access_key_id aws_secret_access_key params in
  let key_equals_value = Util.encode_key_equals_value params in
  let content = String.concat "&" key_equals_value in
  lwt _,s = HC.post ~headers:["Content-Type","application/x-www-form-urlencoded"] ~body:(`String content) (Printf.sprintf "https://%s/" host) in
  let xml = xml_of_string s in
  let session_token = xml_get_content_of "SessionToken" xml
  and secret_access_key = xml_get_content_of "SecretAccessKey" xml
  and access_key_id = xml_get_content_of "AccessKeyId" xml
  and expiration = xml_get_content_of "Expiration" xml
  in match session_token,secret_access_key,access_key_id,expiration with
    | Some session_token,Some secret_access_key,Some access_key_id,Some expiration ->
      let expiration = Scanf.sscanf expiration "%d-%d-%dT%d:%d:%d.%dZ" Util.make in
      return {session_token; secret_access_key; access_key_id; expiration}
    | _ -> raise Invalid_credential
end
