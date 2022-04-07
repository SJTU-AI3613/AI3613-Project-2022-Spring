.PHONY: build clean format test grade submit-project1 docker-build docker-run

build:
	@mkdir -p build && cd build && cmake .. && make -j
	
clean:
	@rm -rf build

format:
	@find . \( -name "*.h" -o -iname "*.cc" \) |xargs clang-format -i -style=file

test:
	@cd build && ctest --out-on-failure

grade:
	@bash scripts/grade.sh

submit-project1:
	@git archive --format tar.gz --output "project1_submission.tar.gz" project1

docker-build:
	@docker build . -t naivedb

docker-run:
	@docker run -it --rm -v `pwd`:`pwd` -w `pwd` naivedb