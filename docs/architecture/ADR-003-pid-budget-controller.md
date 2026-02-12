# ADR-003: PID Controller for Fee Budget Management

## Status

Accepted

## Context

The gateway must manage query fee budgets to balance:

1. **Cost efficiency** - Minimize fees paid to indexers
2. **Query success rate** - Ensure queries succeed by offering competitive fees
3. **Responsiveness** - Adapt quickly to market conditions

Static fee budgets fail because:

- Too low: Indexers reject queries, degraded service
- Too high: Overpaying, wasted budget
- Market conditions change: Indexer fees fluctuate based on demand

We need a dynamic system that automatically adjusts fee budgets based on observed success rates.

## Decision

Implement a PID (Proportional-Integral-Derivative) controller to dynamically adjust fee budgets based on query success rate.

### PID Controller Overview

The PID controller continuously adjusts the fee budget using three terms:

```
adjustment = Kp * error + Ki * integral + Kd * derivative

where:
  error = target_success_rate - actual_success_rate
  integral = sum of past errors
  derivative = rate of error change
```

- **P (Proportional)**: Immediate response to current error
- **I (Integral)**: Corrects persistent bias over time
- **D (Derivative)**: Dampens oscillations, smooths response

### Implementation

See `src/budgets.rs` for implementation:

```rust
pub struct Budgeter {
    controller: PidController,
    decay_buffer: DecayBuffer,
    budget_per_query: f64,
}

impl Budgeter {
    pub fn feedback(&self, success: bool) {
        self.decay_buffer.record(success);
        let success_rate = self.decay_buffer.success_rate();
        let adjustment = self.controller.update(success_rate);
        self.budget_per_query *= adjustment;
    }
}
```

### Decay Buffer

Success rate is calculated using exponential decay to weight recent observations more heavily:

```
weighted_sum = sum(success_i * decay^i)
weighted_count = sum(decay^i)
success_rate = weighted_sum / weighted_count
```

This provides:

- Fast response to changing conditions
- Natural forgetting of stale data
- Bounded memory usage

## Consequences

### Positive

1. **Self-tuning**: Budget automatically converges to optimal level
2. **Adaptive**: Responds to market changes without manual intervention
3. **Stable**: PID controllers are well-understood and tuneable
4. **Observable**: Budget changes can be monitored via metrics

### Negative

1. **Tuning required**: PID gains (Kp, Ki, Kd) must be tuned for the system
2. **Oscillation risk**: Poorly tuned controller can oscillate
3. **Complexity**: More complex than static budgets
4. **Cold start**: Initial budget must be set heuristically

## Tuning Parameters

Current parameters (may need adjustment based on production data):

| Parameter | Value | Purpose                                   |
| --------- | ----- | ----------------------------------------- |
| Kp        | 0.1   | Proportional gain - immediate response    |
| Ki        | 0.01  | Integral gain - bias correction           |
| Kd        | 0.05  | Derivative gain - oscillation damping     |
| Target    | 0.95  | Target success rate (95%)                 |
| Decay     | 0.99  | Decay factor for success rate calculation |

## Alternatives Considered

### Static Budget (Rejected)

```rust
const BUDGET_PER_QUERY: GRT = GRT::from_wei(1_000_000);
```

Problems:

- Cannot adapt to market conditions
- Requires manual intervention to change
- Either overpays or fails queries

### Threshold-based Adjustment (Rejected)

```rust
if success_rate < 0.9 { budget *= 1.1; }
if success_rate > 0.95 { budget *= 0.9; }
```

Problems:

- Oscillates around thresholds
- Step changes cause instability
- No derivative term to dampen oscillations

### Machine Learning Model (Rejected)

Train a model to predict optimal budget based on features.

Problems:

- Requires training data
- Black box behavior
- Overkill for this use case

## References

- [PID Controller (Wikipedia)](https://en.wikipedia.org/wiki/PID_controller)
- [Control Theory for Software Engineers](https://blog.acolyer.org/2015/05/01/feedback-control-for-computer-systems/)
