"""Count records flowing out of named statements (apply after probe.py, which defines the Counter).

COUNT="t_123=label one;t_456=label two" wraps `let t_123 = <expr>;` as
`let t_123 = (<expr>).inspect(counter)`; the binary prints `[count] label one=N` at exit.
"""

import os
import sys
from pathlib import Path

from _edit import find, statement_end

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
for item in filter(None, os.environ["COUNT"].split(";")):
    name, label = item.split("=", 1)
    start = find(code, f"let {name} = ") + len(f"let {name} = ")
    end = statement_end(code, start)
    counter = f'{{ let mut c = arrangement_probe::Counter::new("{label}"); move |_| c.bump() }}'
    code = code[:start] + f"({code[start:end]}).inspect({counter})" + code[end:]
main_rs.write_text(code)
