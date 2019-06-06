from twitterinfrastructure import ex_mod as ex

def test_ex_func():
    temp = ex.ex_func(5)
    print("Example function test (within example module) ran!")
    assert temp == 10
