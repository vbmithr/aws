open Lwt
open SDB

let creds = Creds.({ aws_access_key_id     = Keys.k;
                     aws_secret_access_key = Keys.c })

let list_domains () =
  lwt l, _ = SDB.list_domains ~creds () in
  Lwt_io.printf "Domains: [ %s ]\n%!" (String.concat " " l)

let create_domain name = SDB.create_domain ~creds name

let delete_domain name = SDB.delete_domain ~creds name

let get_attributes domain item =
  lwt l = SDB.get_attributes ~encoded:false ~creds ~domain ~item () in
  Lwt_list.iter_s (fun (n, v) -> match v with
    | None -> Lwt.return ()
    | Some v -> Lwt_io.printf "%s -> %s\n" n v) l

(* let rec select token expr = *)
(*   lwt l, nxt = SDB.select ?token ~encoded:false ~creds expr in *)
(*   List.iter (fun (name, attrs) -> *)
(*     print_endline name; *)
(*     List.iter (fun (name, value) -> Lwt_io.printf "   %s -> %s\n" name *)
(*       (match value with Some s -> s | None -> "none")) attrs) l; *)
(*   (match nxt with *)
(*     | None -> Lwt_io.printf "no token\n" *)
(*     | Some _ -> select nxt expr) *)

let _ = Lwt_main.run
  (
    lwt () = Lwt_io.printf "Listing domains...\n%!" in
    lwt () = list_domains () in
    let domain = Sys.argv.(1) in
    lwt () = create_domain domain in
    lwt () = Lwt_io.printf "Created domain %s\n%!" Sys.argv.(1) in
    lwt () = Lwt_io.printf "Now listing domain again...\n%!" in
    lwt () = list_domains () in
    lwt () = Lwt_io.printf "Now deleting %s\n%!" Sys.argv.(1) in
    lwt () = delete_domain domain in
    lwt () = Lwt_io.printf "Printing domain again!\n%!" in
    list_domains ()
  )

(* let _ =  *)
(*   Lwt_main.run  *)
(*     (select None Sys.argv.(1)) *)
