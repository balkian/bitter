PYVERSIONS=3.4 2.7
PYMAIN=$(firstword $(PYVERSIONS))
NAME=bitter
REPO=balkian
VERSION=$(shell cat $(NAME)/VERSION)


all: build run

dockerfiles: $(addprefix Dockerfile-,$(PYVERSIONS))

Dockerfile-%: Dockerfile.template
	sed "s/{{PYVERSION}}/$*/" Dockerfile.template > Dockerfile-$*

build: $(addprefix build-, $(PYMAIN))

buildall: $(addprefix build-, $(PYVERSIONS))

build-%: Dockerfile-%
	docker build -t '$(REPO)/$(NAME):$(VERSION)-python$*' -f Dockerfile-$* .;

test: $(addprefix test-,$(PYMAIN))

testall: $(addprefix test-,$(PYVERSIONS))

test-%: build-%
	docker run --rm -w /usr/src/app/ -v $$PWD/tests/credentials.json:/usr/src/app/tests/credentials.json --entrypoint=/usr/local/bin/python -ti '$(REPO)/$(NAME):$(VERSION)-python$*' setup.py test --addopts "-vvv -s --pdb" ;

pip_test-%:
	docker run --rm -v $$PWD/dist:/dist/ -ti python:$* pip install /dist/$(NAME)-$(VERSION).tar.gz ;

dist/$(NAME)-$(VERSION).tar.gz:
	docker run --rm -ti -v $$PWD:/usr/src/app/ -w /usr/src/app/ python:$(PYMAIN) python setup.py sdist;

sdist: dist/$(NAME)-$(VERSION).tar.gz


upload-%: test-%
	docker push '$(REPO)/$(NAME):$(VERSION)-python$(PYMAIN)'

upload: testall $(addprefix upload-,$(PYVERSIONS))
	docker tag '$(REPO)/$(NAME):$(VERSION)-python$(PYMAIN)' '$(REPO)/$(NAME):$(VERSION)'
	docker tag '$(REPO)/$(NAME):$(VERSION)-python$(PYMAIN)' '$(REPO)/$(NAME)'

clean:
	@docker ps -a | awk '/$(REPO)\/$(NAME)/{ split($$2, vers, "-"); if(vers[1] != "${VERSION}"){ print $$1;}}' | xargs docker rm 2>/dev/null|| true
	@docker images | awk '/$(REPO)\/$(NAME)/{ split($$2, vers, "-"); if(vers[1] != "${VERSION}"){ print $$1":"$$2;}}' | xargs docker rmi 2>/dev/null|| true

upload_git:
	git commit -a
	git tag ${VERSION}
	git push --tags origin master

pip_upload:
	python setup.py sdist upload ;

pip_test: $(addprefix pip_test-,$(PYVERSIONS))

run: build
	docker run --rm -p 5000:5000 -ti '$(REPO)/$(NAME):$(VERSION)-python$(PYMAIN)'

.PHONY: test test-% build-% build test test_pip run
