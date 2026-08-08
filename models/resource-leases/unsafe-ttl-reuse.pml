/* Regression model: expiry incorrectly frees a still-running host. */
mtype = { FREE, ACTIVE };
mtype lease_state = FREE;
bool running[2];

proctype Execution(byte me) {
    atomic {
        lease_state == FREE;
        lease_state = ACTIVE;
        running[me] = true;
        assert(!(running[0] && running[1]))
    }
    if
    :: atomic { running[me] = false; lease_state = FREE }
    :: atomic { lease_state = FREE } /* unsafe TTL reuse */
    fi
}

init {
    atomic {
        run Execution(0);
        run Execution(1)
    }
}

ltl exclusive { [] !(running[0] && running[1]) }
