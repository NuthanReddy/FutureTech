# Variable-Volume Spice Jar: Prototype Brief

**Status:** Development brief. Mechanism selection is open.
**Nominal capacity:** 100 mL usable capacity; proposed target, not a demonstrated result.
**First experiment:** Removable follower disc.
**Drive study:** Single-stage peripheral screw, if bottom operation is required.
**Engineering record:** [Engineering findings](Brainstorming.md).
**Concept drawings:** [Text concept designs](ConceptDesigns.md).

## 1. Objective

Reduce headspace as spice is consumed.
Determine whether this improves storage performance compared with a conventional gasketed jar.
Keep operation, cleaning, and manufacture simple.

Do not claim longer shelf life before comparison results are available.
Do not approve a drive before its travel and packaging are defined.

This brief uses short sentences and consistent technical names.
Full ASD-STE100 conformity requires an approved vocabulary review.

## 2. Scope

Use dry ground spices for the first experiment.
Use turmeric, chilli, coriander, and cumin powder as representative materials.

Keep food-contact surfaces primarily glass, stainless steel, and silicone.
Use a removable gasketed lid.
Keep food-contact parts accessible for washing.
Use no disposable bag, motor, battery, sensor, or powered vacuum pump.

Whole seeds, flakes, oily mixtures, and dishwasher use require later evaluation.
Production tooling is outside the first experiment.

## 3. Requirements and open decisions

| Item | Status |
|---|---|
| Reduced headspace | Functional objective |
| Simple operation and cleaning | Required |
| Separation of spice and mechanism | Required for a driven version |
| Glass lid | Proposed common feature |
| Stainless-steel body | Permitted; top-view visibility only |
| Glass body | Permitted; side visibility available |
| Bottom collar | Preference to confirm before drive selection |
| 100 mL usable capacity | Proposed product target |
| 50 mm floor stroke | Provisional drive-study target |
| 8 to 12 mm base | Not an approved constraint |
| 85% to 90% external-volume utilization | Not established; no acceptance requirement |
| Three cam followers or multiple thread starts | Options, not requirements |

Define maximum external dimensions before selecting a drive.
Confirm whether side visibility or top visibility is sufficient.
Define user torque and minimum storage-performance improvement before acceptance testing.

## 4. First experiment: follower disc

Use a standard straight-sided glass jar with a closed bottom.
Use its gasketed lid as the main storage closure.
Place an SS304 follower disc above the spice.
Fit one removable silicone perimeter lip.
Provide a lifting feature.
Use a removable lifting hook if the disc cannot be reached.

The disc must not require a bottom drive.
It must not require a custom open-bottom glass body.
Allow trapped air to escape during placement.
Do not add a valve or second lip without an identified need.

### 4.1 Filling and use

1. Remove the lid.
2. Remove the follower disc.
3. Fill the jar below the defined fill line.
4. Place the disc above the spice.
5. Tilt the disc or release its lip to let air escape.
6. Seat the disc without compressing the spice.
7. Install the lid.

Remove the disc before spooning.
Repeat steps 4 to 7 after use.

### 4.2 Cleaning

1. Remove the lid and disc.
2. Empty the jar.
3. Remove the disc seal and lid gasket.
4. Wash the food-contact parts.
5. Inspect the seal grooves for residue.
6. Dry all parts.
7. Install the seals.
8. Assemble the jar.

Define the permitted washing method for the selected materials.
Do not advertise dishwasher suitability at this stage.

## 5. Optional drive study

Start only if reduced headspace provides a useful benefit.
Confirm the bottom collar requirement first.

Study a single-stage peripheral screw with a non-rotating SS304 floor.
Provide a smooth stationary sealing bore.
Keep the threaded interface below the food barrier.
Provide a separate anti-rotation guide.
Provide bearing support and axial retention for the rotating member.
Provide positive end stops and moving-part retention.

Do not assume a thread provides precise guidance or prevents back-driving.
Do not assume an inverted cup fits inside a shallow base.

### 5.1 Required geometry

Show dimensioned LOW and HIGH assembly sections.
Include the floor, skirt, thread engagement, bearings, guides, collar, and stops.
Show the seal boundary and all air paths.
Show assembly and food-contact part removal.

For a fixed base nut:

```text
Working skirt length >= stroke + retained thread engagement
```

A 50 mm stroke and 10 mm engagement need about 60 mm of working skirt.
This example does not specify a safe engagement length.
Calculate engagement from the selected load, material, guidance, and wear allowance.
Show where the skirt fits at LOW.

Add nested stages only if the approved envelope requires them.
Define stage sequence, reverse movement, retained overlap, and accumulated clearance.
Count all guides, retainers, and bearings in the bill of materials.

### 5.2 Lead and operation

```text
Lead = number of starts * pitch
Turns = stroke / lead
```

A 10 mm lead gives five turns over 50 mm.
A three-start thread with that lead has approximately 3.33 mm pitch.
Select lead from measured torque and user effort.
Do not specify multiple starts only to obtain five turns.

Adjust the floor with the lid open.
Provide controlled air exchange below the floor.
Keep dust and washing water out of the drive.

Use a position indicator that does not repeat ambiguously each revolution.
Do not use a fixed collar mark as the only multi-turn position reference.

### 5.3 Overload control

Evaluate a flexible-detent clutch after measuring normal operating torque.
Set release torque below measured damage thresholds with an appropriate margin.
Check the resulting axial force across the full stroke.
Keep positive end stops.
Do not use clutch slipping to set the normal floor position.

### 5.4 Alternative drives

Keep a dry central screw and a descending top piston as comparison options.
A central screw below the food barrier is permitted.
A top piston needs space for its stem and removal.

A three-point cam remains an option.
It needs anti-rotation guidance and retained followers.
Show the complete long-track carrier or extending structure.
Do not assume 50 mm stroke fits inside an 8 to 12 mm ring.

## 6. Capacity and material specification

State capacity below the fill line.
State external diameter and total height separately.
Specify whether dimensions include the lid and collar.

```text
Cylinder volume_mL = pi * diameter_mm^2 * height_mm / 4000
```

| Reference geometry | Volume |
|---|---:|
| 45 mm internal diameter; 55 mm usable height | 87.5 mL |
| 50 mm external diameter; 65 mm body height, excluding lid | 127.6 mL |
| 55 mm external diameter; 72 mm body height, excluding lid | 171.1 mL |

The reference usable capacity is not 100 mL.
The reference utilization is approximately 51% to 69%, not 85% to 90%.
Calculate final volume from CAD and measure actual capacity.

Use SS304 as the starting food-contact metal.
Consider SS316L only if service conditions require it.
Specify bore straightness, ovality, surface finish, and seal clearance.
Do not assume deep drawing supplies the required bore tolerance.

Use compliant food-contact silicone for removable seals.
Confirm compliance for the intended market.
Evaluate spice oils, cleaning chemicals, staining, aroma retention, and gas transmission.
A second lip is an option, not a guaranteed barrier.

Use printed parts only for appropriate mechanism experiments.
Do not use unqualified printed parts for consumable-spice storage.
Do not consume spice used in unqualified prototypes.
Select dry-region polymers for wear, creep, temperature, and cleaning exposure.

## 7. Test plan

These tests produce development evidence.
They do not establish regulatory certification.
Approve test conditions and measurement methods before execution.

### 7.1 Storage and user comparison

Compare the conventional jar with the follower-disc jar.
Use the same spice batch, fill quantity, and storage conditions.
Match lid-opening duration, opening frequency, and spice removal.
Record ambient temperature and humidity.

Measure aroma retention, caking, and moisture uptake over a defined storage period.
Use repeated samples.
Define the required improvement before reviewing results.
Record disc removal, spoon access, retrieval, placement, and cleaning effort.

### 7.2 Seals and leakage

Check lid leakage and disc sealing separately.
For a driven jar, also check the floor seal and base joints.
Check any valve if fitted.
Compare pressure decay and controlled moisture ingress with the conventional jar.
Pressure decay alone does not establish long-term gas or moisture transmission.

For a driven jar, test LOW, MID, and HIGH.
Inspect both movement directions for folding, tearing, abrasion, and powder passage.
Collect escaped powder from the dry region without including seal residue.
Use a measurement method that can resolve the specified mass limit.

### 7.3 Drive and overload

Measure torque at empty, partial, and full loads.
Repeat with clean and contaminated seals.
Measure floor tilt across its diameter throughout the stroke.
Check back-driving, accidental movement, and countertop stability.

Introduce controlled obstructions at the seal.
Check clutch release, retention, structural loads, and recovery.
Evaluate coarse particles separately before adding them to the product scope.
Do not conduct destructive overload tests during normal food handling.

### 7.4 Cleaning and durability

Apply oily residue for a separate cleaning challenge.
Leave the residue for 48 hours.
Record removal, washing, drying, and assembly times separately.
Inspect seal grooves, piston undersides, joints, and vents.

Measure drive wear, seal wear, torque drift, and clutch release drift.
Include repeated seal removal and lid operation.
Include representative kitchen temperature and humidity conditions.

## 8. Provisional drive-test targets

These are development targets, not demonstrated results.
Confirm their suitability before the drive test.

| Item | Proposed target |
|---|---|
| Dry movement | 100 full cycles without fracture or loss of retention |
| Floor stroke | 50 mm |
| Full-stroke turns | Five to seven; revise from torque results |
| Floor tilt across diameter | Preferred maximum 1.0 mm; initial limit 1.5 mm |
| Powder passage | Less than 0.1 g per 100 cycles; no visible powder on drive surfaces |
| Initial powder test | 500 full cycles |
| Clutch overload test | 500 slip events without fracture or loss of function |
| Durability | 5,000 defined movement cycles |
| Routine disassembly and assembly | Within two minutes; exclude washing and drying |

Operating torque, allowable axial load, and storage improvement limits remain open.
Do not declare acceptance while these limits are undefined.

## 9. Decision gates

| Gate | Evidence required |
|---|---|
| Storage benefit | Defined improvement over the conventional jar |
| User handling | Disc or piston use is practical; cleaning is accessible |
| Drive packaging | Complete LOW and HIGH sections; capacity and external dimensions |
| Drive function | Acceptable torque, guidance, retention, and back-driving control |
| Seal function | Acceptable powder passage and separate leakage results |
| Protection | Overload release below damage thresholds; positive stops |
| Durability | Defined cycle targets met without unacceptable wear |
| Commercial review | Supplier quotations and customer evidence |

Do not start production tooling before all applicable gates are complete.
If the drive cannot fit, revise the envelope or choose a simpler option.
Do not add stages only to preserve an unsupported shallow-base claim.

## 10. Deliverables and cost

Provide native parametric CAD and STEP files for a driven prototype.
Provide dimensioned sections, an exploded view, and an individual-part bill of materials.
Provide tolerances, assembly instructions, and cleaning instructions.
Provide capacity, torque, tilt, leakage, powder, overload, and durability results.
Record failures and proposed corrections.

Obtain supplier quotations after the geometry and process are defined.
Include tooling, assembly, inspection, rejects, packaging, freight, taxes, and warranty.
The earlier INR 300 factory-cost objective is not an approved requirement.
No prototype cost, production cost, retail price, or part-count limit is established.
