(* SQS API *)
(* william@corefarm.com *)

module Make = functor (HC : Aws_sigs.HTTP_CLIENT) ->
struct
  module C = CalendarLib.Calendar
  module P = CalendarLib.Printer.CalendarPrinter
  module X = Xml



  open Lwt
  open Creds

  module Util = Aws_util


  exception Error of string

  let print = Printf.printf
  let sprint = Printf.sprintf
  let service = "queue"

  let error_msg body =
    match X.xml_of_string body with
      | X.E ("Response",_,(X.E ("Errors",_,[X.E ("Error",_,[
        X.E ("Code",_,[X.P code]);
        X.E ("Message",_,[X.P message])
      ])]))::_) ->
        `Error message
      | _ -> `Error "unknown message"



(* xml handling utilities *)
  let queue_url_of_xml = function
    | X.E ("QueueUrl",_ , [ X.P url ]) -> url
    | _ ->
      raise (Error ("QueueUrlResponse"))

  let list_queues_response_of_xml = function
    | X.E("ListQueuesResponse", _, [
      X.E("ListQueuesResult",_,items) ;
      _ ;
    ]) ->
      List.map queue_url_of_xml items
    | _ ->
      raise (Error "ListQueuesRequestsResponse")

  let create_queue_response_of_xml = function
    | X.E("CreateQueueResponse", _, kids) -> (
      match kids with
        | [_ ; X.E ("QueueUrl",_, [ X.P url ])] -> (
          url
        )
        | _ -> raise (Error "CreateQueueResponse.queueurl")
    )
    | _ -> raise (Error "CreateQueueResponse")

  type message =
      {
        message_id : string ;
        receipt_handle : string ;
        body : string }

  let message_of_xml encoded = function
    | X.E ("Message",
           _,
           X.E ("MessageId", _, [ X.P message_id ])
           :: X.E ("ReceiptHandle", _, [ X.P receipt_handle ])
           :: X.E ("MD5OfBody", _ , _)
           :: X.E ("Body", _, [ X.P body ]) :: attributes
    ) -> { message_id ; receipt_handle ; body = (if encoded then Util.base64_decoder body else body) }

    | _ -> raise (Error "ReceiveMessageResult.message")

  let receive_message_response_of_xml ~encoded = function
    | X.E ("ReceiveMessageResponse",
           _,
           [
             X.E("ReceiveMessageResult",_ , items) ;
             _ ;
           ]) -> List.map (message_of_xml encoded) items

    | _ -> raise (Error "ReceiveMessageResponse")

  let send_message_response_of_xml = function
    | X.E ("SendMessageResponse",
           _,
           [
             X.E("SendMessageResult",_ ,
                 [
                   X.E ("MD5OfMessageBody", _, _) ;
                   X.E ("MessageId", _, [ X.P message_id ])
                 ]) ;
             _ ;
           ]) -> message_id

    | _ -> raise (Error "SendMessageResponse")


(* create queue *)
  let create_queue ?(default_visibility_timeout=30) creds queue_name =

    let url, params = Util.signed_request ~service ~creds
      ~params:[
        "Action", ["CreateQueue"] ;
        "QueueName", [queue_name] ;
        "DefaultVisibilityTimeout", [string_of_int default_visibility_timeout] ;
      ] () in
    try_lwt
  let ps = Uri.encoded_of_query params in
  print "posting request on %s: %s\n" url ps ;
  lwt header, body = HC.post ~body:(`String ps) url in


  let xml = X.xml_of_string body in
  return (`Ok (create_queue_response_of_xml xml))
  with HC.Http_error (code, _, body) -> print "Error %d %s\n" code body ; return (error_msg body)

(* list existing queues *)
  let list_queues ?prefix creds =

    let url, params = Util.signed_request ~service ~creds
      ~params:(("Action", ["ListQueues"])
       :: (match prefix with
           None -> []
         | Some prefix -> [ "QueueNamePrefix", [prefix] ])) () in
    try_lwt
  let ps = Uri.encoded_of_query params in
  lwt header, body = HC.post ~body:(`String ps) url in

  let xml = X.xml_of_string body in
  return (`Ok (list_queues_response_of_xml xml))
    with HC.Http_error (code, _, body) -> print "Error %d %s\n" code body ; return (error_msg body)

(* get messages from a queue *)
  let receive_message ?(attribute_name="All") ?(max_number_of_messages=1) ?(visibility_timeout=30) ?(encoded=true) creds queue_url =
    let url, params = Util.signed_request ~service ~creds ~http_uri:queue_url
      ~params:[
        "Action", ["ReceiveMessage"] ;
        "AttributeName", [attribute_name] ;
        "MaxNumberOfMessages", [string_of_int max_number_of_messages] ;
        "VisibilityTimeout", [string_of_int visibility_timeout] ;
      ] () in
    try_lwt
  lwt header, body = HC.post ~body:(`String (Uri.encoded_of_query params)) url in

  let xml = X.xml_of_string body in
  return (`Ok (receive_message_response_of_xml ~encoded xml))
  with HC.Http_error (_, _, body) -> return (error_msg body)

(* delete a message from a queue *)

  let delete_message creds queue_url receipt_handle =
    let url, params = Util.signed_request ~service ~creds ~http_uri:queue_url
      ~params:[
        "Action", ["DeleteMessage"] ;
        "ReceiptHandle", [receipt_handle]
      ] () in
    try_lwt
  lwt header, body = HC.post ~body:(`String (Uri.encoded_of_query params)) url in
  ignore (header) ;
  ignore (body);
  return (`Ok ())
    with HC.Http_error (_, _, body) -> return (error_msg body)

(* send a message to a queue *)

  let send_message creds queue_url ?(encoded=true) body =
    let url, params = Util.signed_request ~service ~creds ~http_uri:queue_url
      ~params:[
        "Action", ["SendMessage"] ;
        "MessageBody", [if encoded then Util.base64 body else body]
      ] () in
    try_lwt
  lwt header, body = HC.post ~body:(`String (Uri.encoded_of_query params)) url in

  let xml = X.xml_of_string body in
  return (`Ok (send_message_response_of_xml xml))
  with HC.Http_error (_, _, body) ->  return (error_msg body)
end
