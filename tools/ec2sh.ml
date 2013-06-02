(* ec2 toplevel *)
open Lwt
open Printf

let run = Lwt_main.run

let creds = Creds.({ aws_access_key_id     = Keys.k;
                     aws_secret_access_key = Keys.c })

let string_of_opt = function
  | None -> "-"
  | Some s -> s

let string_of_error = function
  | `Error (code,msg) -> Printf.sprintf "%s: %s" code msg
  | `InsufficientInstanceCapacity msg -> Printf.sprintf "InsufficientInstanceCapacity: %s" msg

let describe_regions () =
  let regions = run (EC2.describe_regions creds) in
  List.iter (fun (id, name) -> Printf.printf "%s\t%s\n" id name) regions

let describe_spot_price_history ?instance_type ?region () =
  let history = run (EC2.describe_spot_price_history ?region ?instance_type creds) in
  List.iter (
    fun h ->
      Printf.printf "%s\t%s\t%0.4f\t%0.3f\n"
        h#instance_type
        h#product_description
        h#spot_price
        h#timestamp
  ) history


let terminate_instances ?region instance_ids =
  let resp = run (EC2.terminate_instances ?region creds instance_ids) in
  match resp with
    | `Ok killed ->
      List.iter (
        fun i ->
          printf "%s\t%s\t%s\n"
            i#instance_id
            (EC2.string_of_instance_state i#previous_state)
            (EC2.string_of_instance_state i#current_state)
      ) killed
    | (`Error _)
    | (`InsufficientInstanceCapacity _) as err -> print_endline (string_of_error err)

let print_reservation r =
  List.iter (
    fun instance ->
      printf "%s\t%s\t%s\t%s\t%s\t%f\n"
        r#id
        instance#id
        instance#image_id
        (string_of_opt instance#private_dns_name_opt)
        (string_of_opt instance#dns_name_opt)
        instance#launch_time
  ) r#instances

let describe_instances ?region instance_ids =
  let resp = run (EC2.describe_instances ?region creds instance_ids) in
  match resp with
    | `Ok reservations ->
      List.iter print_reservation reservations
    | (`Error _)
    | (`InsufficientInstanceCapacity _) as err -> print_endline (string_of_error err)

let run_instances
    ~key_name
    ~region
    ~placement_availability_zone
    ~image_id
    ~min_count
    ~max_count () =

  let resp = run (EC2.run_instances
    creds
    ~key_name
    ~region
    ~placement_availability_zone
    ~image_id
    ~min_count
    ~max_count
  )
  in
  match resp with
    | `Ok reservation ->
      print_reservation reservation
    | (`Error _)
    | (`InsufficientInstanceCapacity _) as err -> print_endline (string_of_error err)


let print_spot_instance_request_descriptions sir_descriptions =
  List.iter (
    fun d ->
      printf "%s\t%s\t%s\t%0.3f\t%s\t%s\t%s\t%s\n"
        d#id
        (string_of_opt d#instance_id_opt)
        (EC2.string_of_spot_instance_request_type d#sir_type)
        d#spot_price
        (EC2.string_of_spot_instance_request_state d#state)
        (match d#image_id_opt with Some image_id -> image_id | None -> "--")
        (match d#key_name_opt with Some key_name -> key_name | None -> "--")
        (String.concat "," d#groups)
  ) sir_descriptions

let request_spot_instances ?region ?key_name ?availability_zone_group sirs =
  let resp = run (EC2.request_spot_instances ?region creds sirs) in
  match resp with
    | `Ok sir_descrs -> print_spot_instance_request_descriptions sir_descrs
    | (`Error _)
    | (`InsufficientInstanceCapacity _) as err-> print_endline (string_of_error err)
let describe_spot_instance_requests ?region sir_ids () =
  let resp = run (EC2.describe_spot_instance_requests ?region creds sir_ids) in
  match resp with
    | `Ok sir_descr -> print_spot_instance_request_descriptions sir_descr
    | (`Error _)
    | (`InsufficientInstanceCapacity _) as err-> print_endline (string_of_error err)
let cancel_spot_instance_requests ?region sir_list =
  let resp = run (EC2.cancel_spot_instance_requests ?region creds sir_list) in
  match resp with
    | `Ok sir_state_list ->
      List.iter (
        fun (sir, state) -> printf "%s\t%s\n"
          sir (EC2.string_of_spot_instance_request_state state)
      ) sir_state_list
    | (`Error _)
    | (`InsufficientInstanceCapacity _) as err -> print_endline (string_of_error err)
