/* Finite safety abstraction: two executions, one physical host.
 * Bounds: one acquisition per execution. No fairness/liveness claim.
 */
mtype = { FREE, ACTIVE, REVOKING, QUARANTINED };
mtype lease_state = FREE;
byte owner = 255;
byte fence = 0;
bool running[2];

inline check_exclusive() {
    assert(!(running[0] && running[1]))
}

proctype Execution(byte me) {
    byte my_fence;
    atomic {
        lease_state == FREE;
        owner = me;
        fence++;
        my_fence = fence;
        lease_state = ACTIVE;
        running[me] = true;
        check_exclusive()
    }
    if
    :: atomic {
           running[me] = false;
           owner == me && fence == my_fence;
           lease_state = FREE;
           owner = 255
       }
    :: atomic {
           owner == me && fence == my_fence;
           lease_state = REVOKING
       };
       if
       :: atomic {
              running[me] = false;
              owner == me && fence == my_fence && lease_state == REVOKING;
              lease_state = FREE;
              owner = 255
          }
       :: atomic {
              owner == me && fence == my_fence && lease_state == REVOKING;
              lease_state = QUARANTINED
          };
          atomic {
              running[me] = false;
              owner == me && fence == my_fence && lease_state == QUARANTINED;
              lease_state = FREE;
              owner = 255
          }
       fi
    fi;
    /* A delayed release from an older fencing generation is a no-op. */
    atomic {
        if
        :: owner == me && fence == my_fence ->
             lease_state = FREE;
             owner = 255
        :: else -> skip
        fi;
        check_exclusive()
    }
}

init {
    atomic {
        run Execution(0);
        run Execution(1)
    }
}

ltl exclusive { [] !(running[0] && running[1]) }
