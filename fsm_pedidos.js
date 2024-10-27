const states = {
    Procesando: { on: { next: 'Preparación' } },
    Preparación: { on: { next: 'Enviado', previous: 'Procesando' } },
    Enviado: { on: { next: 'Entregado', previous: 'Preparación' } },
    Entregado: { on: { next: 'Finalizado', previous: 'Enviado' } },
    Finalizado: { on: { previous: 'Entregado' } }
};

class FiniteStateMachine {
    constructor(state, states) {
        this.state = state;
        this.states = states;
    }

    transition(event) {
        const nextState = this.states[this.state].on[event];
        if (nextState) {
            this.state = nextState;
        } else {
            throw new Error(`Evento no manejado: ${event}`);
        }
    }

    getCurrentState() {
        return this.state;
    }
}

const pedidoFSM = new FiniteStateMachine('Procesando', states);
console.log(`Estado actual: ${pedidoFSM.getCurrentState()}`);

pedidoFSM.transition('next');
console.log(`Estado actual: ${pedidoFSM.getCurrentState()}`);
