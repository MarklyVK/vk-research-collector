#!/usr/bin/env bash

EXPECTED_LEGACY_QUARANTINE_REASON='Runtime-конфигурация не совпадает с immutable run'

deployment_transition_decision() {
  if [[ "${TRANSITION_HEALTH_OK:-0}" != 1 ]]; then
    printf 'Healthcheck deployment transition не пройден.\n' >&2
    return 1
  fi
  if [[ "${TRANSITION_IMAGE_OK:-0}" != 1 ]]; then
    printf 'Immutable image deployment transition не подтверждён.\n' >&2
    return 1
  fi
  if [[ "${TRANSITION_ALEMBIC_OK:-0}" != 1 ]]; then
    printf 'Alembic head deployment transition не подтверждён.\n' >&2
    return 1
  fi
  if [[ "${RUN_STATUS:-}" == failed ]]; then
    printf 'Collection run перешёл в failed.\n' >&2
    return 1
  fi
  if (( ${FINAL_FAILED:-0} > ${BASELINE_FAILED:-0} )); then
    printf 'Количество failed jobs увеличилось.\n' >&2
    return 1
  fi

  if [[ "${RUN_STATUS:-}" == paused_capacity_limit ]]; then
    case "${BASELINE_RUN_STATUS:-}" in
      planned|running|waiting_method_limit|paused_capacity_limit) ;;
      *)
        printf 'До deployment legacy run не был active.\n' >&2
        return 1
        ;;
    esac
    if [[ "${RUN_ERROR_MESSAGE:-}" != "$EXPECTED_LEGACY_QUARANTINE_REASON" ]]; then
      printf 'Capacity pause не является ожидаемой configuration-mismatch quarantine.\n' >&2
      return 1
    fi
    if [[ "${TRANSITION_DATA_UNCHANGED:-0}" != 1 ]]; then
      printf 'Quarantine изменила campaign, jobs или checkpoints.\n' >&2
      return 1
    fi
    if [[ "${FINAL_RUNNING_LEASES:-1}" != 0 || "${FINAL_RUNNING:-1}" != 0 ]]; then
      printf 'После quarantine остались running leases.\n' >&2
      return 1
    fi
    printf 'legacy run quarantined as expected\n'
    return 0
  fi

  if (( ${FINAL_COMPLETED:-0} <= ${BASELINE_COMPLETED:-0} \
    && ${FINAL_RUNNING:-0} == 0 \
    && ${FINAL_RETRY:-0} == 0 \
    && ${FINAL_PENDING:-0} > 0 )); then
    printf 'Worker не показал прогресс и не находится в running/retry ожидании.\n' >&2
    return 1
  fi

  printf 'collection transition healthy\n'
}
