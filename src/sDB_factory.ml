(* SDB API *)
(* william@corefarm.com *)
(* vb@luminar.eu.org 2013 *)

module C = CalendarLib.Calendar
module P = CalendarLib.Printer.CalendarPrinter
module X = Xml
module Util = Aws_util

open Lwt
open Creds

let b64dec_if encoded s = if encoded then Util.base64_decoder s else s
let b64enc_if encode s  = if encode  then Util.base64 s else s

let sprint = Printf.sprintf
let service = "sdb"


(* XML readers *)

let domain_or_next_of_xml = function
  | X.E ("DomainName", _, [ X.P domain_name ]) -> `D domain_name
  | X.E ("NextToken", _, [ X.P next_token ]) -> `N next_token
  | _ -> raise (Failure "ListDomainsResult.domain")

let list_domains_response_of_xml = function
  | X.E ("ListDomainsResponse", _, [
    X.E ("ListDomainsResult", _, domain_or_next_list );
    _ ]) ->
    let domain_names, next_tokens = List.fold_left (
      fun (domain_names, next_tokens) domain_or_next_xml ->
        match domain_or_next_of_xml domain_or_next_xml with
          | `D domain_name -> domain_name :: domain_names, next_tokens
          | `N next_token -> domain_names, next_token :: next_tokens
    ) ([],[]) domain_or_next_list in
    (* preseve the order of domains in response *)
    let domain_names = List.rev domain_names in
    domain_names,
    (* check that we have no more than one NextToken *)
    (match next_tokens with
      | [next_token] -> Some next_token
      | [] -> None
      | _ -> raise (Failure "ListDomainsResponse: more than one NextToken")
    )
  | _ -> raise (Failure "ListDomainsResult")

let attributes_of_xml encoded = function
  | X.E ("Attribute", _, [
    X.E ("Name", _, [ X.P name ]);
    X.E ("Value", _, [ X.P value ]);
  ]) ->
    b64dec_if encoded name,  Some (b64dec_if encoded value)
  | X.E ("Attribute", _, [
    X.E ("Name", _, [ X.P name ]);
    X.E ("Value", _, [ ]);
  ]) ->
    b64dec_if encoded name, None
  | _ -> raise (Failure "Attribute 1")

let get_attributes_response_of_xml encoded = function
  | X.E ("GetAttributesResponse", _, [
    X.E ("GetAttributesResult", _, attributes);
    _;
  ]) -> List.map (attributes_of_xml encoded) attributes
  | _ -> raise (Failure "GetAttributesResponse")

let attrs_of_xml encoded = function
  | X.E ("Attribute", _ , children) ->
    ( match children with
      | [ X.E ("Name", _, [ X.P name ]) ;
          X.E ("Value", _, [ X.P value ]) ;
        ] ->  b64dec_if encoded name, Some (b64dec_if encoded value)
      | [ X.E ("Name", _, [ X.P name ]) ;
          X.E ("Value", _, [ ]) ;
        ] -> b64dec_if encoded name, None
      | l -> raise (Failure (sprint "fat list %d" (List.length l)))
    )
  | _ -> raise (Failure "Attribute 3")

let rec item_of_xml encoded acc token = function
  | [] -> (acc, token)
  | X.E ("Item", _, (X.E ("Name", _, [ X.P name ]) :: attrs)) :: nxt ->
    let a = List.map (attrs_of_xml encoded) attrs in
    item_of_xml encoded (((b64dec_if encoded name), a) :: acc) token nxt
  | X.E ("NextToken", _, [ X.P next_token ]) :: _ -> acc, (Some next_token)
  | _ -> raise (Failure "Item")

let select_of_xml encoded = function
  | X.E ("SelectResponse", _, [
    X.E ("SelectResult", _, items);
    _ ;
  ]) -> item_of_xml encoded [] None items
  | _ -> raise (Failure "SelectResponse")

(* SDB data model *)

type domain = string
type attr = string
type attr_value = string option
type item = string
type item_value = (attr * attr_value) list

module type S = sig
  exception Error of int * string

  val list_domains : ?token:string -> creds:Creds.t -> unit -> (string list * string option) Lwt.t

  val create_domain : creds:Creds.t -> domain -> unit Lwt.t

  val delete_domain : creds:Creds.t -> domain -> unit Lwt.t

  val put_attributes : ?replace:bool -> ?encode:bool -> creds:Creds.t ->
    domain:domain -> item:item -> attrs:item_value -> unit -> unit Lwt.t

  val batch_put_attributes : ?replace:bool -> ?encode:bool -> creds:Creds.t ->
    domain:domain -> (item * item_value) list -> unit Lwt.t

  val get_attributes : ?encoded:bool -> ?attribute:attr -> creds:Creds.t
    -> domain:domain -> item:item -> unit -> item_value Lwt.t

  val delete_attributes : ?encode:bool -> creds:Creds.t -> domain:domain -> item:item
    -> attrs:item_value -> unit Lwt.t

  val select : ?token:string -> ?consistent:bool -> ?encoded:bool -> creds:Creds.t -> string
    -> ((item * item_value) list * string option) Lwt.t

  val select_where_attribute_equals : ?token:string -> ?consistent:bool -> ?encoded:bool
    -> creds:Creds.t -> domain:domain -> attr:attr -> value:string -> unit
    -> ((item * item_value) list * string option) Lwt.t
end

module Make(HC : Aws_sigs.HTTP_CLIENT) : S = struct

  exception Error of int * string

  let raise_sdb_error code body =
    match X.xml_of_string body with
      | X.E ("Response",_, (
        X.E ("Errors",_, [
          X.E ("Error",_,[
            X.E ("Code",_,[X.P code]);
            X.E ("Message",_,[X.P message]);
            _
          ])])) :: _ ) -> raise_lwt Error (int_of_string code, message)
      | _ -> raise_lwt Error (code, body)

  (* list all domains *)
  let list_domains ?token ~creds () =
    let url, params = Util.signed_request ~service ~creds
      ~params:(("Action", ["ListDomains"]) :: match token with
          None -> []
        | Some t -> [ "NextToken", [t] ]) () in

    try_lwt
       lwt header, body = HC.post
         ~body:(`String (Uri.encoded_of_query ~value_component:`RFC3986 params)) url in
       let xml = X.xml_of_string body in
       return (list_domains_response_of_xml xml)
    with HC.Http_error (code, _, body) -> raise_sdb_error code body

  (* create domain *)
  let create_domain ~creds name =
    let url, params = Util.signed_request ~service ~creds ~params:[
      "Action", ["CreateDomain"] ;
      "DomainName", [name]
    ] () in

    try_lwt
      lwt header, body = HC.post
        ~body:(`String (Uri.encoded_of_query ~value_component:`RFC3986 params)) url in
      return ()
    with HC.Http_error (code, _, body) -> raise_sdb_error code body

  (* delete domain *)
  let delete_domain ~creds name =
    let url, params = Util.signed_request ~service ~creds ~params:[
      "Action", ["DeleteDomain"] ;
      "DomainName", [name]
    ] () in

    try_lwt
       lwt header, body = HC.post
         ~body:(`String (Uri.encoded_of_query ~value_component:`RFC3986 params)) url in
       return ()
    with HC.Http_error (code, _, body) -> raise_sdb_error code body

  (* put attributes *)
  let put_attributes ?(replace=false) ?(encode=true) ~creds ~domain ~item ~attrs () =
    let _, attrs' = List.fold_left (
      fun (i, acc) (name, value_opt) ->
        let value_s =
          match value_opt with
            | Some value -> b64enc_if encode value
            | None -> ""
        in
        let value_p = sprint "Attribute.%d.Value" i, [value_s] in
        let name_p = sprint "Attribute.%d.Name" i, [b64enc_if encode name] in
        let acc =
          name_p :: value_p :: (
            if replace then
              (sprint "Attribute.%d.Replace" i, ["true"]) :: acc
            else
              acc
          ) in
        i+1, acc
    ) (1, []) attrs in
    let url, params = Util.signed_request ~service ~creds
      ~params:(("Action", ["PutAttributes"])
       :: ("DomainName", [domain])
       :: ("ItemName", [b64enc_if encode item])
       :: attrs') () in
    try_lwt
      lwt header, body = HC.post
        ~body:(`String (Uri.encoded_of_query ~value_component:`RFC3986 params)) url in
      return ()
    with HC.Http_error (code, _, body) -> raise_sdb_error code body

  (* batch put attributes *)
  let batch_put_attributes ?(replace=false) ?(encode=true) ~creds ~domain items =
    let _, attrs' = List.fold_left
      (fun (i, acc) (item_name, attrs) ->
         let item_name_p = sprint "Item.%d.ItemName" i, [b64enc_if encode item_name] in
         let _, acc = List.fold_left (
           fun (j, acc) (name, value_opt) ->
             let name_p = sprint "Item.%d.Attribute.%d.Name" i j, [b64enc_if encode name] in
             let value_s =
               match value_opt with
                 | Some value -> b64enc_if encode value
                 | None -> "" in
             let value_p = sprint "Item.%d.Attribute.%d.Value" i j, [value_s] in
             let acc' = name_p :: value_p ::
               if replace then
                  (sprint "Item.%d.Attribute.%d.Replace" i j, ["true"]) :: acc
                else
                  acc
             in
             j+1, acc'
         ) (1, item_name_p :: acc) attrs in
         i+1, acc
      ) (1, []) items in

    let url, params = Util.signed_request ~service ~creds
      ~params:(("Action", ["BatchPutAttributes"])
       :: ("DomainName", [domain])
       :: attrs') () in
    try_lwt
      lwt header, body = HC.post
        ~body:(`String (Uri.encoded_of_query ~value_component:`RFC3986 params)) url 
      in return ()
    with HC.Http_error (code, _, body) ->  raise_sdb_error code body

  (* get attributes *)
  let get_attributes ?(encoded=true) ?attribute ~creds ~domain ~item () =
    let attribute_name_p =
      match attribute with
        | None -> []
        | Some attribute_name ->
            [ "AttributeName", [b64enc_if encoded attribute_name] ]
    in
    let url, params = Util.signed_request ~service ~creds
      ~params:(["Action", ["GetAttributes"];
                "DomainName", [domain];
                "ItemName", [b64enc_if encoded item]] @ attribute_name_p) ()
    in
    try_lwt
      lwt header, body = HC.post ~body:(`String (Uri.encoded_of_query params)) url in
      let xml = X.xml_of_string body in
      return (get_attributes_response_of_xml encoded xml)
    with HC.Http_error (code, _, body) ->  raise_sdb_error code body

  (* delete attributes *)
  let delete_attributes ?(encode=true) ~creds ~domain ~item ~attrs =
    let _, attrs' = List.fold_left (
      fun (i, acc) (name, value) ->
        let name_p = sprint "Attribute.%d.Name" i, [b64enc_if encode name] in
        match value with
          | Some v -> let value_p = sprint "Attribute.%d.Value" i, [b64enc_if encode v] in
                      i+1, name_p :: value_p :: acc
          | None    -> i+1, name_p :: acc
    ) (0,[]) attrs in
    let url, params = Util.signed_request ~service ~creds
      ~params:(("Action", ["DeleteAttributes"])
       :: ("DomainName", [domain])
       :: ("ItemName", [b64enc_if encode item])
       :: attrs') () in
    try_lwt
       lwt header, body = HC.post
         ~body:(`String (Uri.encoded_of_query ~value_component:`RFC3986 params)) url in
       return ()
    with HC.Http_error (code, _, body) -> raise_sdb_error code body

  (* select: TODO if [encode=true], encode references to values in the
     select [expression].  This might not be easy, as the [expression]
     will have to be deconstructed (parsed). Alternatively,
     [expression] is replaced with an expression type, which would
     make value substitutions easier. Neither would work for numerical
     constraints.  *)

  let select ?token ?(consistent=false) ?(encoded=true) ~creds expression =
    let url, params = Util.signed_request ~service ~creds
      ~params:(("Action", ["Select"])
       :: ("SelectExpression", [expression])
       :: ("ConsistentRead", [sprint "%B" consistent])
       :: (match token with
         | None -> []
         | Some t -> [ "NextToken", [t] ])) () in
    try_lwt
      let uri_query_component = Uri.encoded_of_query ~value_component:`RFC3986 params in
      lwt header, body = HC.post
        ~body:(`String uri_query_component) url in
      let xml = X.xml_of_string body in
      return (select_of_xml encoded xml)
    with HC.Http_error (code, _, body) -> raise_sdb_error code body

  (* select all records where attribute [name] equals [value] *)
  let select_where_attribute_equals ?token ?(consistent=false) ?(encoded=true)
      ~creds ~domain ~attr ~value () =
    let expression = sprint "select * from `%s` where `%s` = %S"
      domain (b64enc_if encoded attr) (b64enc_if encoded value) in
    select ?token ~consistent ~encoded ~creds expression

end
