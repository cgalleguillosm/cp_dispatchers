#include <cstring>
#include <string>
#include <vector>

#include "ortools/base/integral_types.h"
#include "ortools/base/logging.h"
#include "ortools/base/stringprintf.h"
#include "ortools/constraint_solver/constraint_solver.h"
#include "ortools/constraint_solver/constraint_solveri.h"
#include "ortools/util/string_array.h"


namespace operations_research {
    namespace {
        class HeuristicSearch : public DecisionBuilder {
        public:
            explicit
            HeuristicSearch(const std::vector<IntervalVar *> &vars) :
                    vars_(vars), markers_(vars.size(), kint64min) {
                _first_exec = true;
            }

            virtual
            ~HeuristicSearch() override {}

            virtual Decision *
            Next(Solver *const s) {
                int64 best_est = kint64max; // smallest EST
                int64 best_lct = kint64max; // smallest EST

                // candidate intervals for scheduling
                std::vector<int> support;

                // number of non-eligible, non-bound, intervals
                int refuted = 0;
                int selected = -1;

                // the first solution mimic the greedy heuristic
                if (_first_exec) {
                    double min_score1 = kint64max;
                    double min_score2 = kint64max;
                    int64 cur_pos = kint64max;
                    // Look for candidate variables
                    for (int i = 0; i < vars_.size(); ++i) {
                        IntervalVar *const v = vars_[i];
                        // check if the interval may exist and is not bound
                        if (v->MayBePerformed() && v->StartMax() > v->StartMin()) {
                            // check if the interval is not posponed and starts earlier than
                            // the current minimum EST
                            if (v->StartMin() >= markers_[i]
                                && v->StartMin() < best_est) {
                                // update the current minimum EST
                                best_est = v->StartMin();
                                best_lct = v->EndMax();
                                // reset the candiate intervals
                                support.clear();
                                // Register the interval as the only candidate
                                support.push_back(i);

                                selected = i;
                                cur_pos = i;
                            }
                                // check if the interval is not posponed and its EST is equal to
                                // the current minimum EST
                            else if (v->StartMin() >= markers_[i]
                                     && v->StartMin() == best_est) {
                                // add a candidate interval
                                support.push_back(i);

                                if (i < cur_pos) {
                                    selected = i;
                                    cur_pos = i;
                                }
                            } else {
                                refuted++;
                            }
                        }
                    }
                    // If no candidate interval was found, then either all intervals have
                    // been scheduled or the problem is infeasible
                    if (support.size() == 0 || selected == -1) {
                        if (refuted == 0) {
                            // The first search attempt is over
                            _first_exec = false;
                            // If there is no posponed, unscheduled interval, then succeed
                            return nullptr;
                        } else {
                            // Otherwise fail
                            s->Fail();
                        }
                    }
                }

                // not first solution - STD search
                {
                    for (int i = 0; i < vars_.size(); ++i) {

                        IntervalVar *const v = vars_[i];

                        if (v->MayBePerformed() && v->StartMax() > v->StartMin()) {
                            if (v->StartMin() >= markers_[i]
                                && (v->StartMin() < best_est
                                    || (v->StartMin() == best_est
                                        && v->EndMax() < best_lct))) {
                                best_est = v->StartMin();
                                best_lct = v->EndMax();
                                selected = i;
                            } else {
                                refuted++;
                            }
                        }
                    }
                    // TODO(user) : remove this crude quadratic loop with
                    // reversibles range reduction.
                    if (selected == -1) {
                        if (refuted == 0) {
                            return nullptr;
                        } else {
                            s->Fail();
                        }
                    }
                }

                IntervalVar *var = vars_[selected];
                int64 est = var->StartMin();
                // return s->RevAlloc ( new ScheduleOrPostpone (var, est, &markers_[selected]));
                return s->MakeScheduleOrPostpone(var, est, &markers_[selected]);
            }

            virtual std::string
            DebugString() const {
                return "HeuristicSearch()";
            }

            virtual void
            Accept(ModelVisitor *const visitor) const {
                visitor->BeginVisitExtension(ModelVisitor::kVariableGroupExtension);
                visitor->VisitIntervalArrayArgument(ModelVisitor::kIntervalsArgument,
                                                    vars_);
                visitor->EndVisitExtension(ModelVisitor::kVariableGroupExtension);
            }

        private:
            const std::vector<IntervalVar *> vars_;
            std::vector <int64> markers_;
            bool _first_exec;

        };
    }

// namespace
    DecisionBuilder *Solver::MakeHeuristicSearch(const std::vector<IntervalVar *> &vars) {
        return RevAlloc(new HeuristicSearch(vars));
    }
}