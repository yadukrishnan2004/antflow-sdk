package sdk

import "fmt"

type WorkflowBuilder struct{
	name string
	app  *App
}

type StepBuilder struct {
	name     string
	wType    WorkflowType
	steps    []workflowStep
	app       *App
}

func (b *WorkflowBuilder) Chain() *StepBuilder {
	return &StepBuilder{
		name: b.name,
		wType: ChainWorkflow,
		app :b.app,
	}
}


func (b *WorkflowBuilder) Independent() *StepBuilder {
	return &StepBuilder{
		name: b.name,
		wType: IndependentWorkflow,
		app: b.app,
	}
}

func (b *WorkflowBuilder) Saga() *StepBuilder {
	return &StepBuilder{
		name: b.name,
		wType: SagaWorkflow,
		app: b.app,
	}
}

func (b *StepBuilder) Step(name string, fn WorkflowFunc) *StepBuilder {
	b.steps = append(b.steps, workflowStep{name: name, fn: fn})
	return b
}

func (b *StepBuilder) SagaStep(name string, fn WorkflowFunc, compensationName string, compensationFn WorkflowFunc) *StepBuilder {
	b.steps = append(b.steps, workflowStep{
		name:             name,
		fn:               fn,
		compensationName: compensationName,
		compensationFn:   compensationFn,
	})
	return b
}

func (b *StepBuilder) Done() {
    b.app.track(b)
}

func (b *StepBuilder) register() error {
	if len(b.steps) == 0 {
		return fmt.Errorf("workflow '%s' has no steps", b.name)
	}
	return b.app.registry.register(b.name, b.wType, b.steps)
}
