open Lwt

open SDB

let creds = Creds.({ aws_access_key_id     = Keys.k;
                     aws_secret_access_key = Keys.c })

let list_domains () =
  SDB.list_domains creds () >>= function
  | `Ok (l,_) -> List.iter print_endline l ; return ()
  | `Error (code, msg) -> Lwt_io.printf "Panic: %s\n" msg

let create_domain name =
  SDB.create_domain creds name >>= function
  | `Ok -> return ()
  | `Error (code, msg) -> Lwt_io.printf "Panic: %s\n" msg

let delete_domain name =
  SDB.delete_domain creds name >>= function
  | `Ok -> return ()
  | `Error (code, msg) -> Lwt_io.printf "Panic: %s\n" msg

let get_attributes domain item =
  SDB.get_attributes ~encoded:false ~creds ~domain ~item () >>= function
  | `Ok l -> Lwt_list.iter_s (fun (n, v) ->
    match v with
      | None -> Lwt.return ()
      | Some v -> Lwt_io.printf "%s -> %s\n" n v) l
  | `Error (code, msg) -> Lwt_io.printf "Panic: %s\n" msg

let rec select token expr =
  SDB.select ?token ~encoded:false ~creds expr >>= function
  | `Ok (l, nxt) ->
    List.iter (fun (name, attrs) ->
      print_endline name;
      List.iter (fun (name, value) -> Printf.printf "   %s -> %s\n" name
        (match value with Some s -> s | None -> "none")) attrs) l;
    (match nxt with
        None -> Lwt_io.printf "no token\n"
      | Some _ -> select nxt expr)
  | `Error (code, msg) -> Lwt_io.printf "Panic: %s\n" msg


let _ = Lwt_main.run
  (
    let () = Printf.printf "Listing domains...\n%!" in
    lwt () = list_domains () in
    let domain = Sys.argv.(1) in
    lwt () = create_domain domain in
    let () = Printf.printf "Created domain %s\n" Sys.argv.(1) in
    let () = Printf.printf "Now listing domain again...\n" in
    lwt () = list_domains () in
    let () = Printf.printf "Now deleting %s\n" Sys.argv.(1) in
    lwt () = delete_domain domain in
    let () = Printf.printf "Printing domain again!\n" in
    list_domains ()
  )

(* let _ =  *)
(*   Lwt_main.run  *)
(*     (select None Sys.argv.(1)) *)
