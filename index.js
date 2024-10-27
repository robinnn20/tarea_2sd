const states = {
    Red : { on : { next: 'Green', previous: 'Yellow' } },
    Green: { on : { next: 'Yellow', previous: 'Red' } },
    Yellow: { on: { next: 'Red', previous: 'Green' } }
};

class FiniteStateMachine {
    constructor(state, states) {
        this.state = state;
        this.states = states;
    }

    async transition(event) {
        const nextState = this.states[this.state].on[event];
        if(nextState) {
            this.state = nextState;
        } else {
            throw new Error('Unhandled state');
        }
    }

    getCurrentState() {
        return this.state;
    }
}

const semaphore = new FiniteStateMachine('Red', states);
console.log(`Estado actual: ${semaphore.getCurrentState()}`);
semaphore.transition('next');
console.log(`Estado actual: ${semaphore.getCurrentState()}`);
