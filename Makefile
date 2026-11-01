.PHONY: backup-create backup-verify

backup-create:
	$(if $(strip $(FILE)),,$(error FILE is required))
	docker compose exec -T telegram-adapter python -m app.backup create "$(FILE)" $(if $(filter 1,$(OVERWRITE)),--overwrite,)

backup-verify:
	$(if $(strip $(FILE)),,$(error FILE is required))
	docker compose exec -T telegram-adapter python -m app.backup verify "$(FILE)"
