module switchboard::switchboard_init_action {
  use switchboard::errors;
  use switchboard::switchboard;
  use std::signer;

  public entry fun run(state: signer) {
    assert!(@switchboard == signer::address_of(&state), errors::PermissionDenied());
    assert!(!switchboard::exist(signer::address_of(&state)), errors::StateAlreadyExists());
    switchboard::state_create(&state);
  }
  
  public entry fun add_switchboard_events(state: signer) {
    assert!(@switchboard == signer::address_of(&state), errors::PermissionDenied());
    assert!(!switchboard::switchboard_events_exists(), errors::StateAlreadyExists());
    switchboard::switchboard_events_create(&state);
  }

  public entry fun add_switchboard_read_events(state: signer) {
    assert!(@switchboard == signer::address_of(&state), errors::PermissionDenied());
    assert!(!switchboard::switchboard_read_events_exists(), errors::StateAlreadyExists());
    switchboard::switchboard_read_event_create(&state);
  }
}
